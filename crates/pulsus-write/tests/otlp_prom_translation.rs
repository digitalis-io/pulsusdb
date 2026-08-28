//! Issue #461: the OTLP metrics receiver must name and label series the way
//! Prometheus v3.13.0's OTLP receiver does.
//!
//! Every expected answer in
//! `tests/fixtures/otlp-metrics/prom-translation/cases.json` was **captured
//! from a running `prom/prometheus:v3.13.0`** by [`capture_cases_json`]
//! below, never typed by hand. The inputs (payloads, strategies, tags) are
//! ours; the `expect` half is a paste of the reference's own bytes, so a
//! disagreement between this corpus and our parser is our defect until
//! proven otherwise.
//!
//! Re-capture with five containers, one per effective configuration:
//!
//! ```text
//! PULSUS_CAPTURE_PROM_UNDERSCORE_ESCAPING_WITH_SUFFIXES=127.0.0.1:39460 \
//! PULSUS_CAPTURE_PROM_NO_UTF8_ESCAPING_WITH_SUFFIXES=127.0.0.1:39461 \
//! PULSUS_CAPTURE_PROM_UNDERSCORE_ESCAPING_WITHOUT_SUFFIXES=127.0.0.1:39462 \
//! PULSUS_CAPTURE_PROM_NO_TRANSLATION=127.0.0.1:39463 \
//! PULSUS_CAPTURE_PROM_PROMOTE_SCOPE=127.0.0.1:39464 \
//!   cargo test -p pulsus-write --test otlp_prom_translation -- --ignored --nocapture
//! ```
//!
//! **The reference's admission window is head-relative**, not wall-clock:
//! the floor is `head.maxTime - 60min`, so a fixed historical fixture is
//! admitted by a fresh server and refused by a warmed one with
//! `400 'out of bounds'` (one line per rejected emitted series). The
//! capture therefore anchors every push at run time — case `i` at
//! `floor(now) + i` seconds — and rewrites the committed payloads back to
//! the fixed [`REFERENCE_TS_MS`] afterwards, recording each expected sample
//! as an **offset** from it. Ledgered as `otlp-reference-admission-window`.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::time::Duration;

use axum::body::Body;
use axum::http::{HeaderMap, StatusCode};
use prost::Message as _;
use pulsus_config::{ExpHistogramMode, OtlpTranslationStrategy};
use pulsus_write::protocols::otlp_metrics::{self, MetricIngestSettings};
use pulsus_write::{Backpressure, FlushWait, MetricSink, ParsedMetrics};
use serde_json::{Value, json};

/// The fixed `timeUnixNano` every committed payload carries, and the base
/// every expected sample's `offset_ms` is measured from.
const REFERENCE_TS_MS: i64 = 1_787_920_000_000;

const CASES_JSON: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/otlp-metrics/prom-translation/cases.json"
);

/// The eleven transformations the reference applies that we did not, each
/// of which must be covered by a minimal pair in the corpus (AC3).
const TRANSFORMATIONS: &[&str] = &[
    "name-escaping",
    "unit-suffix",
    "total-suffix",
    "ratio-suffix",
    "label-sanitization",
    "collision-merge",
    "empty-value-delete",
    "job-instance",
    "resource-attrs-not-promoted",
    "scope-not-promoted",
    "target-info",
];

// ---------------------------------------------------------------------
// Corpus model

fn load_cases() -> Value {
    let raw =
        std::fs::read_to_string(CASES_JSON).unwrap_or_else(|e| panic!("read {CASES_JSON}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {CASES_JSON}: {e}"))
}

fn strategy_of(case: &Value) -> OtlpTranslationStrategy {
    case["strategy"]
        .as_str()
        .expect("case.strategy")
        .parse()
        .expect("case.strategy is one of the four reference spellings")
}

fn settings_of(case: &Value) -> MetricIngestSettings {
    MetricIngestSettings {
        exp_histogram_mode: ExpHistogramMode::Classic,
        translation_strategy: strategy_of(case),
        promote_scope_metadata: case["promote_scope_metadata"]
            .as_bool()
            .expect("case.promote_scope_metadata"),
        promql_lookback_ms: 300_000,
    }
}

/// One emitted series+sample, rendered so a mismatch reads as text.
fn render(metric_name: &str, labels: &[(String, String)], value: f64, unix_milli: i64) -> String {
    let inner: Vec<String> = labels.iter().map(|(k, v)| format!("{k}={v:?}")).collect();
    format!(
        "{metric_name}{{{}}} = {:?} @ {unix_milli}",
        inner.join(", "),
        value
    )
}

/// What our parser produced for one case, as a sorted multiset of rendered
/// rows. A stale sample renders its bit pattern, not `NaN`, so two distinct
/// NaNs can never compare equal.
fn our_rows(parsed: &ParsedMetrics) -> Vec<String> {
    let labels_of: BTreeMap<(&str, u64), Vec<(String, String)>> = parsed
        .series
        .iter()
        .map(|s| {
            (
                (s.metric_name.as_ref(), s.fingerprint),
                s.labels
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            )
        })
        .collect();
    let mut rows: Vec<String> = parsed
        .samples
        .iter()
        .map(|p| {
            let labels = labels_of
                .get(&(p.metric_name.as_ref(), p.fingerprint))
                .expect("every sample's series is registered");
            if p.value.is_nan() {
                format!(
                    "{}{{{}}} = nan:{:#x} @ {}",
                    p.metric_name,
                    labels
                        .iter()
                        .map(|(k, v)| format!("{k}={v:?}"))
                        .collect::<Vec<_>>()
                        .join(", "),
                    p.value.to_bits(),
                    p.unix_milli
                )
            } else {
                render(&p.metric_name, labels, p.value, p.unix_milli)
            }
        })
        .collect();
    rows.sort();
    rows
}

/// What the reference produced for one case, in the identical rendering.
/// `value: null` means the series exists but answers no instant query —
/// the reference's stale marker — which we store as `STALE_NAN_BITS`.
fn expected_rows(expect: &Value) -> Vec<String> {
    let mut rows: Vec<String> = expect["series"]
        .as_array()
        .expect("expect.series")
        .iter()
        .map(|s| {
            let name = s["metric_name"].as_str().expect("metric_name");
            let labels: Vec<(String, String)> = s["labels"]
                .as_object()
                .expect("labels")
                .iter()
                .map(|(k, v)| (k.clone(), v.as_str().expect("label value").to_string()))
                .collect();
            let unix_milli = REFERENCE_TS_MS + s["offset_ms"].as_i64().expect("offset_ms");
            match s["value"].as_str() {
                Some(v) => render(
                    name,
                    &labels,
                    v.parse::<f64>().expect("reference value parses as f64"),
                    unix_milli,
                ),
                None => format!(
                    "{name}{{{}}} = nan:{:#x} @ {unix_milli}",
                    labels
                        .iter()
                        .map(|(k, v)| format!("{k}={v:?}"))
                        .collect::<Vec<_>>()
                        .join(", "),
                    pulsus_model::STALE_NAN_BITS
                ),
            }
        })
        .collect();
    rows.sort();
    rows
}

fn parse_case(case: &Value) -> Result<ParsedMetrics, pulsus_write::LogsIngestError> {
    let body = serde_json::to_vec(&case["payload"]).expect("payload re-serializes");
    let request = otlp_metrics::decode_json(&body).expect("corpus payloads are valid OTLP/JSON");
    otlp_metrics::parse(&request, REFERENCE_TS_MS * 1_000_000, settings_of(case))
}

// ---------------------------------------------------------------------
// AC1 / AC5a-d — the corpus

#[test]
fn corpus_matches_prometheus_v3_13_0() {
    let doc = load_cases();
    let mut failures: Vec<String> = Vec::new();
    let mut checked = 0usize;
    for case in doc["cases"].as_array().expect("cases") {
        let expect = &case["expect"];
        if expect["kind"] != "accept" {
            continue;
        }
        checked += 1;
        let id = case["id"].as_str().expect("id");
        match parse_case(case) {
            Ok(parsed) => {
                let ours = our_rows(&parsed);
                let theirs = expected_rows(expect);
                if ours != theirs {
                    failures.push(format!(
                        "case {id}\n  prometheus v3.13.0: {theirs:#?}\n  pulsusdb: {ours:#?}"
                    ));
                }
            }
            Err(err) => failures.push(format!("case {id}: rejected by us, accepted there: {err}")),
        }
    }
    assert!(checked > 0, "the corpus must contain accepting cases");
    assert!(
        failures.is_empty(),
        "{} of {checked} cases diverge from the reference:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

// ---------------------------------------------------------------------
// AC2 — the whole-request rejections, through the public handler

/// A `MetricSink` that records nothing: the reject cases must never reach
/// it, and the accept control proves the route works.
#[derive(Default)]
struct CountingSink {
    admitted: std::sync::Mutex<Vec<ParsedMetrics>>,
}

impl MetricSink for CountingSink {
    fn admit(&self, batch: ParsedMetrics) -> Result<(), Backpressure> {
        self.admitted.lock().expect("sink lock").push(batch);
        Ok(())
    }

    fn admit_flush(&self, batch: ParsedMetrics) -> Result<FlushWait, Backpressure> {
        self.admitted.lock().expect("sink lock").push(batch);
        Ok(FlushWait::new(async { Ok(()) }))
    }
}

/// The ingest handlers' hand-rolled `google.rpc.Status { code, message }`
/// protobuf (the private type in `ingest/http.rs`; this binary defines its
/// own decode-only copy of the identical wire shape, as
/// `pulsus-server/tests/api_conformance.rs` already does).
#[derive(Clone, PartialEq, ::prost::Message)]
struct Status {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
}

async fn post_metrics(
    sink: &CountingSink,
    body: Vec<u8>,
    settings: MetricIngestSettings,
) -> (StatusCode, Vec<u8>) {
    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json".parse().expect("header"));
    let response = pulsus_write::ingest_metrics(sink, headers, Body::from(body), settings).await;
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .expect("read response body");
    (status, bytes.to_vec())
}

#[tokio::test]
async fn reference_rejections_are_whole_request_400() {
    let doc = load_cases();
    let mut failures: Vec<String> = Vec::new();
    let mut checked = 0usize;
    for case in doc["cases"].as_array().expect("cases") {
        let expect = &case["expect"];
        if expect["kind"] != "reject" {
            continue;
        }
        checked += 1;
        let id = case["id"].as_str().expect("id");
        // The reference's answer, recorded for the ledger: `500` with a
        // bare `text/plain` body. Ours is `400`/`code = 3` with the same
        // message — `otlp-name-reject-status-400`.
        assert_eq!(
            expect["reference_status"].as_i64(),
            Some(500),
            "case {id}: the captured reference status"
        );
        let required = expect["message"].as_str().expect("expect.message");

        let sink = CountingSink::default();
        let body = serde_json::to_vec(&case["payload"]).expect("payload re-serializes");
        let (status, bytes) = post_metrics(&sink, body, settings_of(case)).await;
        let decoded = Status::decode(bytes.as_slice()).expect("google.rpc.Status body");
        if status != StatusCode::BAD_REQUEST || decoded.code != 3 || decoded.message != required {
            failures.push(format!(
                "case {id}: got {status}/code {} {:?}, required 400/code 3 {required:?}",
                decoded.code, decoded.message
            ));
        }
        if !sink.admitted.lock().expect("sink lock").is_empty() {
            failures.push(format!("case {id}: a rejected request reached the sink"));
        }
    }
    assert_eq!(
        checked, 6,
        "the contract names six whole-request rejections"
    );
    assert!(
        failures.is_empty(),
        "{} of {checked} rejecting cases were accepted or misclassified:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[tokio::test]
async fn an_accepted_case_still_reaches_the_sink_through_the_same_route() {
    let doc = load_cases();
    let case = doc["cases"]
        .as_array()
        .expect("cases")
        .iter()
        .find(|c| c["id"] == "counter-total")
        .expect("counter-total is in the corpus");
    let sink = CountingSink::default();
    let body = serde_json::to_vec(&case["payload"]).expect("payload re-serializes");
    let (status, _) = post_metrics(&sink, body, settings_of(case)).await;
    assert_eq!(status, StatusCode::OK);
    let admitted = sink.admitted.lock().expect("sink lock");
    assert_eq!(admitted.len(), 1);
    assert!(!admitted[0].samples.is_empty());
}

// ---------------------------------------------------------------------
// AC3 — every transformation is bound by a minimal pair

/// A case's resource attributes, in wire order, rendered.
fn resource_attrs(case: &Value) -> Vec<(String, String)> {
    case["payload"]["resourceMetrics"][0]["resource"]["attributes"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|kv| {
                    (
                        kv["key"].as_str().expect("attr key").to_string(),
                        kv["value"]["stringValue"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string(),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

/// A case's data-point attributes for its first metric, in wire order.
fn datapoint_attrs(case: &Value) -> Vec<(String, String)> {
    let metric = &case["payload"]["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0];
    for kind in ["gauge", "sum"] {
        if let Some(dp) = metric[kind]["dataPoints"].get(0) {
            return dp["attributes"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .map(|kv| {
                            (
                                kv["key"].as_str().expect("attr key").to_string(),
                                kv["value"]["stringValue"]
                                    .as_str()
                                    .unwrap_or_default()
                                    .to_string(),
                            )
                        })
                        .collect()
                })
                .unwrap_or_default();
        }
    }
    Vec::new()
}

/// The OTLP metric names a case pushes, in wire order.
fn input_metric_names(case: &Value) -> Vec<String> {
    case["payload"]["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
        .as_array()
        .expect("metrics")
        .iter()
        .map(|m| m["name"].as_str().expect("metric name").to_string())
        .collect()
}

/// The captured `(metric_name, labels)` pairs for a case.
fn captured_series(case: &Value) -> Vec<(String, BTreeMap<String, String>)> {
    case["expect"]["series"]
        .as_array()
        .unwrap_or_else(|| panic!("case {} has no captured series", case["id"]))
        .iter()
        .map(|s| {
            (
                s["metric_name"].as_str().expect("metric_name").to_string(),
                s["labels"]
                    .as_object()
                    .expect("labels")
                    .iter()
                    .map(|(k, v)| (k.clone(), v.as_str().expect("value").to_string()))
                    .collect(),
            )
        })
        .collect()
}

/// Non-`target_info` metric names in the captured answer, deduplicated.
fn captured_metric_names(case: &Value) -> Vec<String> {
    let mut names: Vec<String> = captured_series(case)
        .into_iter()
        .map(|(name, _)| name)
        .filter(|name| name != "target_info")
        .collect();
    names.sort();
    names.dedup();
    names
}

/// The label sets of every non-`target_info` series in the captured answer.
fn captured_metric_labels(case: &Value) -> Vec<BTreeMap<String, String>> {
    captured_series(case)
        .into_iter()
        .filter(|(name, _)| name != "target_info")
        .map(|(_, labels)| labels)
        .collect()
}

fn captured_target_info(case: &Value) -> Vec<BTreeMap<String, String>> {
    captured_series(case)
        .into_iter()
        .filter(|(name, _)| name == "target_info")
        .map(|(_, labels)| labels)
        .collect()
}

/// Every JSON path at which `a` and `b` differ. Used to prove a "minimal
/// pair" really is minimal: exactly one input feature may differ, so a case
/// cannot satisfy a predicate by dragging an unrelated change along.
fn json_diff(a: &Value, b: &Value, path: &str, out: &mut Vec<String>) {
    match (a, b) {
        (Value::Object(x), Value::Object(y)) => {
            let mut keys: Vec<&String> = x.keys().chain(y.keys()).collect();
            keys.sort();
            keys.dedup();
            for key in keys {
                match (x.get(key), y.get(key)) {
                    (Some(xv), Some(yv)) => json_diff(xv, yv, &format!("{path}.{key}"), out),
                    _ => out.push(format!("{path}.{key}")),
                }
            }
        }
        (Value::Array(x), Value::Array(y)) => {
            if x.len() != y.len() {
                out.push(path.to_string());
                return;
            }
            for (i, (xv, yv)) in x.iter().zip(y).enumerate() {
                json_diff(xv, yv, &format!("{path}[{i}]"), out);
            }
        }
        _ => {
            if a != b {
                out.push(path.to_string());
            }
        }
    }
}

/// The comparable half of a case: everything that decides the answer.
fn comparable(case: &Value) -> Value {
    json!({
        "strategy": case["strategy"],
        "promote_scope_metadata": case["promote_scope_metadata"],
        "payload": case["payload"],
    })
}

struct Pair<'a> {
    base: &'a Value,
    variant: &'a Value,
}

fn find_pair<'a>(doc: &'a Value, pair_id: &str) -> Pair<'a> {
    let cases = doc["cases"].as_array().expect("cases");
    let find = |suffix: &str| {
        cases
            .iter()
            .find(|c| c["pair"] == json!(format!("{pair_id}:{suffix}")))
            .unwrap_or_else(|| panic!("no {suffix} case for pair {pair_id}"))
    };
    let pair = Pair {
        base: find("base"),
        variant: find("variant"),
    };
    let mut diffs = Vec::new();
    json_diff(
        &comparable(pair.base),
        &comparable(pair.variant),
        "",
        &mut diffs,
    );
    assert_eq!(
        diffs.len(),
        1,
        "pair {pair_id} must differ in exactly one input feature, differs at {diffs:?}"
    );
    pair
}

fn sanitize(key: &str) -> String {
    pulsus_model::canonicalize_label_key(key)
}

/// AC3. Each transformation is bound by a minimal pair whose two cases
/// differ in exactly one input, together with an assertion that their
/// captured outputs differ in the way that transformation dictates — and,
/// for every derived value, that the output equals the value derived from
/// that case's own input.
///
/// **Limit.** A minimal pair proves the answer changes with, and only with,
/// that one input feature, and that the derived values are the reference's.
/// It cannot prove our implementation reaches a correct value by the
/// reference's internal route — a lookup table that happened to agree on
/// every corpus input would be indistinguishable from here.
#[test]
fn cases_json_binds_every_named_transformation() {
    let doc = load_cases();

    // (1) name escaping: the variant stores the OTLP name verbatim, the
    // base stores it with every non-`[A-Za-z0-9:]` run replaced.
    {
        let p = find_pair(&doc, "name-escaping");
        assert_eq!(
            captured_metric_names(p.variant),
            input_metric_names(p.variant)
        );
        let escaped: Vec<String> = input_metric_names(p.base)
            .iter()
            .map(|n| {
                n.split(|c: char| !(c.is_ascii_alphanumeric() || c == ':'))
                    .filter(|t| !t.is_empty())
                    .collect::<Vec<_>>()
                    .join("_")
            })
            .collect();
        assert_eq!(captured_metric_names(p.base), escaped);
        assert_ne!(
            captured_metric_names(p.base),
            captured_metric_names(p.variant)
        );
    }

    // (2) unit suffix: the variant's names are the base's plus exactly the
    // unit token, for two different units.
    for (pair_id, suffix) in [("unit-suffix", "_seconds"), ("unit-suffix-bytes", "_bytes")] {
        let p = find_pair(&doc, pair_id);
        let expected: Vec<String> = captured_metric_names(p.base)
            .iter()
            .map(|n| format!("{n}{suffix}"))
            .collect();
        assert_eq!(captured_metric_names(p.variant), expected, "pair {pair_id}");
    }

    // (3) `_total`, (4) `_ratio`: same shape, and the base's own name must
    // not already end in the suffix (which is what made the round-4 mutant
    // pass a presence check).
    for (pair_id, suffix) in [("total-suffix", "_total"), ("ratio-suffix", "_ratio")] {
        let p = find_pair(&doc, pair_id);
        let base = captured_metric_names(p.base);
        assert!(
            base.iter().all(|n| !n.ends_with(suffix)),
            "pair {pair_id}: the base name already carries {suffix}"
        );
        let expected: Vec<String> = base.iter().map(|n| format!("{n}{suffix}")).collect();
        assert_eq!(captured_metric_names(p.variant), expected, "pair {pair_id}");
    }

    // (5) label-name sanitization: the variant's differing attribute key
    // lands under the reference's prefix, carrying its own value.
    for (pair_id, prefix) in [
        ("label-sanitization-digit", "key_"),
        ("label-sanitization-underscore", "key"),
    ] {
        let p = find_pair(&doc, pair_id);
        let (base_key, _) = datapoint_attrs(p.base)[0].clone();
        let (variant_key, variant_value) = datapoint_attrs(p.variant)[0].clone();
        for labels in captured_metric_labels(p.base) {
            assert_eq!(
                labels.get(&base_key),
                Some(&variant_value),
                "pair {pair_id} base"
            );
        }
        let expected_key = format!("{prefix}{}", sanitize(&variant_key));
        for labels in captured_metric_labels(p.variant) {
            assert_eq!(
                labels.get(&expected_key),
                Some(&variant_value),
                "pair {pair_id} variant"
            );
            assert!(
                !labels.contains_key(&variant_key),
                "pair {pair_id}: raw key survived"
            );
        }
    }

    // (6) collision merge: the base's two keys sanitize apart and stay two
    // labels; the variant's collide into one whose value is the two source
    // values joined with `;` in RAW-KEY byte order.
    {
        let p = find_pair(&doc, "collision-merge");
        let base_attrs = datapoint_attrs(p.base);
        let mut base_keys: Vec<String> = base_attrs.iter().map(|(k, _)| sanitize(k)).collect();
        base_keys.sort();
        base_keys.dedup();
        assert_eq!(base_keys.len(), 2, "the base pair's keys must not collide");
        for labels in captured_metric_labels(p.base) {
            for (key, value) in &base_attrs {
                assert_eq!(labels.get(&sanitize(key)), Some(value));
            }
        }
        let mut variant_attrs = datapoint_attrs(p.variant);
        let merged_key = sanitize(&variant_attrs[0].0);
        assert!(
            variant_attrs.iter().all(|(k, _)| sanitize(k) == merged_key),
            "the variant pair's keys must collide"
        );
        variant_attrs.sort_by(|a, b| a.0.cmp(&b.0));
        let merged_value = variant_attrs
            .iter()
            .map(|(_, v)| v.clone())
            .collect::<Vec<_>>()
            .join(";");
        assert!(merged_value.contains(';'));
        for labels in captured_metric_labels(p.variant) {
            assert_eq!(labels.get(&merged_key), Some(&merged_value));
        }
    }

    // (7) empty-value delete.
    {
        let p = find_pair(&doc, "empty-value-delete");
        let (key, value) = datapoint_attrs(p.base)[0].clone();
        assert!(!value.is_empty());
        assert!(datapoint_attrs(p.variant)[0].1.is_empty());
        for labels in captured_metric_labels(p.base) {
            assert_eq!(labels.get(&sanitize(&key)), Some(&value));
        }
        for labels in captured_metric_labels(p.variant) {
            assert!(!labels.contains_key(&sanitize(&key)));
        }
    }

    // (8) `job`/`instance`, bound to the values derived from the case's own
    // resource: `namespace + "/" + name`, or `name` alone.
    {
        let p = find_pair(&doc, "job");
        for labels in captured_metric_labels(p.base) {
            assert!(
                !labels.contains_key("job"),
                "the base resource has no service.name"
            );
        }
        let attrs: BTreeMap<String, String> = resource_attrs(p.variant).into_iter().collect();
        let expected = attrs.get("service.name").expect("variant has service.name");
        for labels in captured_metric_labels(p.variant) {
            assert_eq!(labels.get("job"), Some(expected));
        }
    }
    {
        let p = find_pair(&doc, "job-namespace");
        for case in [p.base, p.variant] {
            let attrs: BTreeMap<String, String> = resource_attrs(case).into_iter().collect();
            let name = attrs.get("service.name").expect("service.name");
            let expected = match attrs.get("service.namespace") {
                Some(ns) => format!("{ns}/{name}"),
                None => name.clone(),
            };
            for labels in captured_metric_labels(case) {
                assert_eq!(labels.get("job"), Some(&expected), "case {}", case["id"]);
            }
        }
        assert_ne!(
            captured_metric_labels(p.base)[0].get("job"),
            captured_metric_labels(p.variant)[0].get("job")
        );
    }
    {
        let p = find_pair(&doc, "instance");
        for labels in captured_metric_labels(p.base) {
            assert!(!labels.contains_key("instance"));
        }
        let attrs: BTreeMap<String, String> = resource_attrs(p.variant).into_iter().collect();
        let expected = attrs
            .get("service.instance.id")
            .expect("variant has service.instance.id");
        for labels in captured_metric_labels(p.variant) {
            assert_eq!(labels.get("instance"), Some(expected));
        }
    }

    // (9) resource attributes are NOT promoted: every non-identifying
    // resource attribute is absent from the metric series and present on
    // `target_info` carrying its own value.
    {
        let p = find_pair(&doc, "resource-attrs-not-promoted");
        let identifying = ["service.name", "service.namespace", "service.instance.id"];
        let extra: Vec<(String, String)> = resource_attrs(p.variant)
            .into_iter()
            .filter(|(k, _)| !identifying.contains(&k.as_str()))
            .collect();
        assert!(
            !extra.is_empty(),
            "the variant must add a non-identifying attribute"
        );
        for labels in captured_metric_labels(p.variant) {
            for (key, _) in &extra {
                assert!(!labels.contains_key(&sanitize(key)), "{key} was promoted");
            }
        }
        let target = captured_target_info(p.variant);
        assert_eq!(target.len(), 1);
        for (key, value) in &extra {
            assert_eq!(target[0].get(&sanitize(key)), Some(value));
        }
        assert!(
            captured_target_info(p.base).is_empty(),
            "the base resource carries only identifying attributes"
        );
    }

    // (10) scope metadata is not promoted by default. The enabled path is
    // asserted only as far as the reference has been replayed here — see
    // the module note; the full enabled-path binding lands with the
    // scope-promotion measurement.
    {
        let p = find_pair(&doc, "scope-not-promoted");
        for labels in captured_metric_labels(p.base) {
            assert!(
                labels.keys().all(|k| !k.starts_with("otel_scope_")),
                "scope metadata must not be promoted with the knob off"
            );
        }
        let scope = &p.variant["payload"]["resourceMetrics"][0]["scopeMetrics"][0]["scope"];
        let scope_name = scope["name"].as_str().expect("scope name");
        for labels in captured_metric_labels(p.variant) {
            assert_eq!(
                labels.get("otel_scope_name").map(String::as_str),
                Some(scope_name)
            );
        }
    }

    // (11) `target_info` exists only when the resource carries a
    // non-identifying attribute, and its label set is exactly the sanitized
    // non-identifying attributes plus the derived `job`/`instance`.
    {
        let p = find_pair(&doc, "target-info");
        assert!(captured_target_info(p.base).is_empty());
        let target = captured_target_info(p.variant);
        assert_eq!(target.len(), 1, "exactly one target_info series");
        let identifying = ["service.name", "service.namespace", "service.instance.id"];
        let attrs: BTreeMap<String, String> = resource_attrs(p.variant).into_iter().collect();
        let mut expected: BTreeMap<String, String> = attrs
            .iter()
            .filter(|(k, _)| !identifying.contains(&k.as_str()))
            .map(|(k, v)| (sanitize(k), v.clone()))
            .collect();
        assert!(
            !expected.is_empty(),
            "an empty target_info{{}} is not a target_info"
        );
        if let Some(name) = attrs.get("service.name") {
            let job = match attrs.get("service.namespace") {
                Some(ns) => format!("{ns}/{name}"),
                None => name.clone(),
            };
            if !job.is_empty() {
                expected.insert("job".to_string(), job);
            }
        }
        if let Some(instance) = attrs.get("service.instance.id")
            && !instance.is_empty()
        {
            expected.insert("instance".to_string(), instance.clone());
        }
        assert_eq!(target[0], expected, "target_info's label set");
    }
}

/// A companion completeness gate: every id in [`TRANSFORMATIONS`] is
/// exercised by at least one tagged case, so a later edit cannot drop one
/// silently.
#[test]
fn cases_json_tags_cover_every_named_transformation() {
    let doc = load_cases();
    let tagged: Vec<&str> = doc["cases"]
        .as_array()
        .expect("cases")
        .iter()
        .flat_map(|c| c["tags"].as_array().expect("tags"))
        .map(|t| t.as_str().expect("tag"))
        .collect();
    for id in TRANSFORMATIONS {
        assert!(tagged.contains(id), "no case is tagged {id}");
    }
    for tag in &tagged {
        assert!(
            TRANSFORMATIONS.contains(tag),
            "unknown transformation tag {tag}"
        );
    }
}

// ---------------------------------------------------------------------
// The capture harness (`#[ignore]`): regenerates `cases.json`'s `expect`
// half from a live `prom/prometheus:v3.13.0`.

/// Minimal HTTP/1.1 over a bare `TcpStream` — the idiom this repo already
/// uses for live suites (`pulsus-server/tests/prom_api_live.rs`), so the
/// harness adds no dependency.
fn http(endpoint: &str, method: &str, path: &str, body: Option<&[u8]>) -> (u16, Vec<u8>) {
    let mut stream =
        TcpStream::connect(endpoint).unwrap_or_else(|e| panic!("connect {endpoint}: {e}"));
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .expect("read timeout");
    let mut request =
        format!("{method} {path} HTTP/1.1\r\nHost: {endpoint}\r\nConnection: close\r\n");
    if let Some(body) = body {
        request.push_str("Content-Type: application/json\r\n");
        request.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes()).expect("write head");
    if let Some(body) = body {
        stream.write_all(body).expect("write body");
    }
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).expect("read response");
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("HTTP response has a header terminator");
    let head = String::from_utf8_lossy(&raw[..split]).to_string();
    let status: u16 = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|c| c.parse().ok())
        .expect("status line");
    let mut body = raw[split + 4..].to_vec();
    if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        body = dechunk(&body);
    }
    (status, body)
}

/// Un-chunks a `Transfer-Encoding: chunked` body.
fn dechunk(raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut rest = raw;
    loop {
        let Some(eol) = rest.windows(2).position(|w| w == b"\r\n") else {
            break;
        };
        let size =
            usize::from_str_radix(String::from_utf8_lossy(&rest[..eol]).trim(), 16).unwrap_or(0);
        if size == 0 {
            break;
        }
        let start = eol + 2;
        out.extend_from_slice(&rest[start..start + size]);
        rest = &rest[start + size + 2..];
    }
    out
}

fn endpoint_for(case: &Value) -> Option<String> {
    let var = if case["promote_scope_metadata"] == json!(true) {
        "PULSUS_CAPTURE_PROM_PROMOTE_SCOPE"
    } else {
        match case["strategy"].as_str().expect("strategy") {
            "UnderscoreEscapingWithSuffixes" => {
                "PULSUS_CAPTURE_PROM_UNDERSCORE_ESCAPING_WITH_SUFFIXES"
            }
            "NoUTF8EscapingWithSuffixes" => "PULSUS_CAPTURE_PROM_NO_UTF8_ESCAPING_WITH_SUFFIXES",
            "UnderscoreEscapingWithoutSuffixes" => {
                "PULSUS_CAPTURE_PROM_UNDERSCORE_ESCAPING_WITHOUT_SUFFIXES"
            }
            "NoTranslation" => "PULSUS_CAPTURE_PROM_NO_TRANSLATION",
            other => panic!("unknown strategy {other}"),
        }
    };
    std::env::var(var).ok()
}

/// Rewrites every `timeUnixNano` in a payload to `ts_ns`.
fn restamp(value: &mut Value, ts_ns: u64) {
    match value {
        Value::Object(map) => {
            for (key, entry) in map.iter_mut() {
                if key == "timeUnixNano" {
                    *entry = json!(ts_ns.to_string());
                } else {
                    restamp(entry, ts_ns);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(|v| restamp(v, ts_ns)),
        _ => {}
    }
}

/// Regenerates the `expect` half of `cases.json` from a live reference.
/// Ignored by default: it needs five running containers and it rewrites a
/// committed fixture.
#[test]
#[ignore = "captures expectations from live prom/prometheus:v3.13.0 containers"]
fn capture_cases_json() {
    let mut doc = load_cases();
    let now_s = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    // Run-time-relative, one second apart: the reference admits only
    // samples within 60 minutes of its head's maximum timestamp, so a fixed
    // historical anchor is refused by any warmed server.
    let anchor_s = now_s - 120;

    let mut build_info: Option<Value> = None;
    let cases = doc["cases"].as_array_mut().expect("cases");
    for (index, case) in cases.iter_mut().enumerate() {
        let Some(endpoint) = endpoint_for(case) else {
            panic!("no capture endpoint configured for case {}", case["id"]);
        };
        if build_info.is_none() {
            let (_, body) = http(&endpoint, "GET", "/api/v1/status/buildinfo", None);
            build_info = Some(serde_json::from_slice(&body).expect("buildinfo"));
        }
        let ts_s = anchor_s + index as u64;
        let mut payload = case["payload"].clone();
        restamp(&mut payload, ts_s * 1_000_000_000);
        let body = serde_json::to_vec(&payload).expect("payload");
        let (status, response) = http(&endpoint, "POST", "/api/v1/otlp/v1/metrics", Some(&body));

        if status != 200 {
            let text = String::from_utf8_lossy(&response).to_string();
            case["expect"] = json!({
                "kind": "reject",
                "reference_status": status,
                "reference_body": text,
                "message": text.trim_end_matches('\n'),
            });
            continue;
        }

        let path = format!(
            "/api/v1/series?match%5B%5D=%7B__name__%3D~%22.%2B%22%7D&start={ts_s}&end={ts_s}"
        );
        let (series_status, series_body) = http(&endpoint, "GET", &path, None);
        assert_eq!(series_status, 200, "series read for {}", case["id"]);
        let series: Value = serde_json::from_slice(&series_body).expect("series json");
        let mut names: Vec<String> = series["data"]
            .as_array()
            .expect("series data")
            .iter()
            .map(|s| s["__name__"].as_str().expect("__name__").to_string())
            .collect();
        names.sort();
        names.dedup();

        // Values, read with a 1 ms range so the PromQL lookback cannot
        // reach an earlier case's sample of the same series.
        let mut values: BTreeMap<String, String> = BTreeMap::new();
        for name in &names {
            let query = format!("last_over_time({{__name__=\"{name}\"}}[1ms])");
            let path = format!("/api/v1/query?query={}&time={ts_s}", urlencode(&query));
            let (status, body) = http(&endpoint, "GET", &path, None);
            assert_eq!(status, 200, "value read for {name}");
            let answer: Value = serde_json::from_slice(&body).expect("query json");
            for result in answer["data"]["result"].as_array().expect("result") {
                let mut labels: BTreeMap<String, String> = result["metric"]
                    .as_object()
                    .expect("metric")
                    .iter()
                    .map(|(k, v)| (k.clone(), v.as_str().expect("value").to_string()))
                    .collect();
                labels.remove("__name__");
                let key = format!("{name}{labels:?}");
                values.insert(
                    key,
                    result["value"][1]
                        .as_str()
                        .expect("sample value")
                        .to_string(),
                );
            }
        }

        let mut captured: Vec<Value> = Vec::new();
        for entry in series["data"].as_array().expect("series data") {
            let object = entry.as_object().expect("series entry");
            let name = object["__name__"].as_str().expect("__name__").to_string();
            let labels: BTreeMap<String, String> = object
                .iter()
                .filter(|(k, _)| k.as_str() != "__name__")
                .map(|(k, v)| (k.clone(), v.as_str().expect("value").to_string()))
                .collect();
            let value = values.get(&format!("{name}{labels:?}")).cloned();
            captured.push(json!({
                "metric_name": name,
                "labels": labels,
                "offset_ms": 0,
                "value": value,
            }));
        }
        captured.sort_by_key(|c| c.to_string());
        case["expect"] = json!({ "kind": "accept", "series": captured });
    }

    doc["captured_from"] = json!({
        "image": "docker.io/prom/prometheus:v3.13.0",
        "buildinfo": build_info.expect("at least one case")["data"],
        "route": "POST /api/v1/otlp/v1/metrics",
        "read_back": "GET /api/v1/series + GET /api/v1/query last_over_time(..[1ms])",
    });

    let rendered = format!("{}\n", serde_json::to_string_pretty(&doc).expect("render"));
    std::fs::write(CASES_JSON, rendered).unwrap_or_else(|e| panic!("write {CASES_JSON}: {e}"));
    println!(
        "captured {} cases into {CASES_JSON}",
        doc["cases"].as_array().unwrap().len()
    );
}

/// Percent-encodes a PromQL query for a URL query string.
fn urlencode(input: &str) -> String {
    let mut out = String::with_capacity(input.len() * 3);
    for byte in input.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

// ---------------------------------------------------------------------
// AC14 — the delta partial-success wire bytes the ledger publishes

/// Prometheus v3.13.0 answers a delta Sum with a whole-request `500` and
/// discards every valid sibling metric in the batch; PulsusDB rejects the
/// delta metric's data points into OTLP partial success and stores the
/// siblings. This test pins the exact bytes that
/// `docs/benchmarks/metrics-differential-ledger.md`'s
/// `otlp-delta-partial-success` row publishes, so the row and the wire
/// cannot drift apart.
#[tokio::test]
async fn delta_temporality_is_partial_success_with_the_bytes_the_ledger_quotes() {
    let body = serde_json::to_vec(&json!({
        "resourceMetrics": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "deltacase"}}
            ]},
            "scopeMetrics": [{
                "scope": {"name": "gen", "version": "1.2.3"},
                "metrics": [
                    {"name": "delta.count", "unit": "1", "sum": {
                        "dataPoints": [
                            {"asDouble": 1.0, "timeUnixNano": "1787920000000000000"},
                            {"asDouble": 2.0, "timeUnixNano": "1787920001000000000"}
                        ],
                        "aggregationTemporality": 1, "isMonotonic": true}},
                    {"name": "ok.gauge", "unit": "", "gauge": {"dataPoints": [
                        {"asDouble": 9.0, "timeUnixNano": "1787920000000000000"}
                    ]}}
                ]
            }]
        }]
    }))
    .expect("payload");

    let sink = CountingSink::default();
    let (status, response) = post_metrics(&sink, body, MetricIngestSettings::default()).await;
    assert_eq!(status, StatusCode::OK);
    println!(
        "pulsusdb delta response: {} bytes, hex {}",
        response.len(),
        response
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    );

    /// `ExportMetricsServiceResponse { partial_success }`, decode-only.
    #[derive(Clone, PartialEq, ::prost::Message)]
    struct PartialSuccess {
        #[prost(uint64, tag = "1")]
        rejected_data_points: u64,
        #[prost(string, tag = "2")]
        error_message: String,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    struct ExportResponse {
        #[prost(message, optional, tag = "1")]
        partial_success: Option<PartialSuccess>,
    }
    let decoded = ExportResponse::decode(response.as_slice()).expect("export response");
    let partial = decoded.partial_success.expect("partial_success is set");
    assert_eq!(partial.rejected_data_points, 2);
    assert_eq!(
        partial.error_message,
        "metric delta.count: delta temporality is not ingested; send cumulative"
    );

    // The sibling survives, which is the whole justification for the
    // divergence: the reference rolls the batch back and stores nothing.
    let admitted = sink.admitted.lock().expect("sink lock");
    let names: Vec<&str> = admitted[0]
        .samples
        .iter()
        .map(|s| s.metric_name.as_ref())
        .collect();
    assert!(names.contains(&"ok_gauge"), "stored: {names:?}");
    assert!(
        !names.iter().any(|n| n.starts_with("delta")),
        "stored: {names:?}"
    );
}

// ---------------------------------------------------------------------
// AC11 — every divergence has a ledger row, asserted cell by cell

const LEDGER: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../docs/benchmarks/metrics-differential-ledger.md"
);
const API_MD: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/api.md");

/// A ledger row's expectation: the exact cells, plus tokens that must
/// appear in **this** row's prose cell.
struct LedgerRow {
    id: &'static str,
    limit: &'static str,
    our_route: &'static str,
    our_status: &'static str,
    their_route: &'static str,
    their_status: &'static str,
    required: &'static [&'static str],
}

/// The complete canonical set. A row missing from the ledger, an id in the
/// ledger that is not here, a status in the wrong column, or a required
/// token that has drifted out of its own row all fail.
const LEDGER_ROWS: &[LedgerRow] = &[
    LedgerRow {
        id: "`otlp-name-reject-status-400`",
        limit: "—",
        our_route: "`POST /v1/metrics`",
        our_status: "`400`",
        their_route: "`POST /api/v1/otlp/v1/metrics`",
        their_status: "`500`",
        required: &["code: 3", "text/plain; charset=utf-8", "#259"],
    },
    LedgerRow {
        id: "`otlp-delta-partial-success`",
        limit: "—",
        our_route: "`POST /v1/metrics`",
        our_status: "`200`",
        their_route: "`POST /api/v1/otlp/v1/metrics`",
        their_status: "`500`",
        required: &[
            "application/x-protobuf",
            "text/plain; charset=utf-8",
            "rejected_data_points: 2",
            "metric delta.count: delta temporality is not ingested; send cumulative",
            "invalid temporality and type combination for metric",
            "metrics_to_prw.go:224-233",
            "ok_gauge",
        ],
    },
    LedgerRow {
        id: "`otlp-target-info-sample-cap`",
        limit: "`4096`",
        our_route: "`POST /v1/metrics`",
        our_status: "`400`",
        their_route: "`POST /api/v1/otlp/v1/metrics`",
        their_status: "`200`",
        required: &["helper.go:560-604", "code = 3"],
    },
    LedgerRow {
        id: "`otlp-target-info-span-accepted-points-only`",
        limit: "—",
        our_route: "`POST /v1/metrics`",
        our_status: "`200`",
        their_route: "`POST /api/v1/otlp/v1/metrics`",
        their_status: "`200`",
        required: &["findMinAndMaxTimestamps", "metrics_to_prw.go:217"],
    },
    LedgerRow {
        id: "`otlp-duplicate-attribute-key-order`",
        limit: "—",
        our_route: "`POST /v1/metrics`",
        our_status: "`200`",
        their_route: "`POST /api/v1/otlp/v1/metrics`",
        their_status: "`200`",
        required: &["ScratchBuilder.Sort", "12"],
    },
    LedgerRow {
        id: "`otlp-reject-message-escape-syntax`",
        limit: "—",
        our_route: "`POST /v1/metrics`",
        our_status: "`400`",
        their_route: "`POST /api/v1/otlp/v1/metrics`",
        their_status: "`500`",
        required: &["strconv.IsPrint", "printable ASCII"],
    },
    LedgerRow {
        id: "`otlp-reference-admission-window`",
        limit: "`60 min`",
        our_route: "`POST /v1/metrics`",
        our_status: "`200`",
        their_route: "`POST /api/v1/otlp/v1/metrics`",
        their_status: "`400`",
        required: &[
            "head-relative",
            "out of order sample",
            "one `out of bounds\\n` line per rejected emitted series",
        ],
    },
    LedgerRow {
        id: "`promql-expression-depth-cap`",
        limit: "`250`",
        our_route: "`POST /api/v1/query`",
        our_status: "`400`",
        their_route: "`POST /api/v1/query`",
        their_status: "`200`",
        required: &["bad_data", "40af9c2cdc0eda00f3622e867a27f6359f7295f3"],
    },
];

/// The id retired by issue #461: the divergence covers metric-name AND
/// label-name rejections, so the half-naming id must appear nowhere. Built
/// from fragments at run time — spelling it as one literal would make this
/// file its own first hit.
fn retired_ledger_id() -> String {
    ["otlp", "metric", "name", "reject", "status", "400"].join("-")
}

fn ledger_table() -> (Vec<String>, Vec<Vec<String>>) {
    let text = std::fs::read_to_string(LEDGER).unwrap_or_else(|e| panic!("read {LEDGER}: {e}"));
    let mut header: Vec<String> = Vec::new();
    let mut rows: Vec<Vec<String>> = Vec::new();
    for line in text.lines() {
        if !line.starts_with('|') {
            continue;
        }
        let cells: Vec<String> = line
            .trim_matches('|')
            .split('|')
            .map(|c| c.trim().to_string())
            .collect();
        if cells
            .iter()
            .all(|c| c.chars().all(|ch| ch == '-') && !c.is_empty())
        {
            continue;
        }
        if header.is_empty() {
            header = cells;
        } else {
            rows.push(cells);
        }
    }
    assert!(!header.is_empty(), "the ledger must carry a table");
    (header, rows)
}

fn column(header: &[String], name: &str) -> usize {
    header
        .iter()
        .position(|h| h == name)
        .unwrap_or_else(|| panic!("the ledger table has no {name:?} column; header {header:?}"))
}

/// AC11. Each divergence this change introduces has a ledger row, and each
/// of that row's load-bearing fields is asserted in the **named column** it
/// belongs to — not searched for anywhere in the row.
///
/// The distinction is the whole point: a row-wide `contains` stays green
/// when the two statuses are swapped between the two routes, or when our
/// response body is exchanged with the reference's, because every token is
/// still somewhere in the row.
#[test]
fn every_divergence_has_a_ledger_row() {
    let (header, rows) = ledger_table();
    let id_col = column(&header, "Divergence");
    let limit_col = column(&header, "Limit");
    let our_route_col = column(&header, "PulsusDB route");
    let our_status_col = column(&header, "PulsusDB status");
    let their_route_col = column(&header, "Reference route");
    let their_status_col = column(&header, "Reference status");
    let rule_col = column(&header, "Rule, and why we diverge");

    let mut seen: Vec<&str> = Vec::new();
    for want in LEDGER_ROWS {
        let row = rows
            .iter()
            .find(|r| r[id_col] == want.id)
            .unwrap_or_else(|| {
                panic!("the ledger has no row whose Divergence cell is {}", want.id)
            });
        assert_eq!(row[limit_col], want.limit, "{}: Limit cell", want.id);
        assert_eq!(
            row[our_route_col], want.our_route,
            "{}: PulsusDB route cell",
            want.id
        );
        assert_eq!(
            row[our_status_col], want.our_status,
            "{}: PulsusDB status cell",
            want.id
        );
        assert_eq!(
            row[their_route_col], want.their_route,
            "{}: Reference route cell",
            want.id
        );
        assert_eq!(
            row[their_status_col], want.their_status,
            "{}: Reference status cell",
            want.id
        );
        for token in want.required {
            assert!(
                row[rule_col].contains(token),
                "{}: its own rule cell must carry {token:?}",
                want.id
            );
        }
        seen.push(want.id);
    }

    for row in &rows {
        assert!(
            seen.contains(&row[id_col].as_str()),
            "the ledger carries an unlisted divergence {:?}; add it to LEDGER_ROWS",
            row[id_col]
        );
    }
    assert_eq!(rows.len(), LEDGER_ROWS.len());

    // The reference version is named once, in the file's own preamble.
    let text = std::fs::read_to_string(LEDGER).expect("read ledger");
    assert!(
        text.contains("v3.13.0") && text.contains("40af9c2cdc0eda00f3622e867a27f6359f7295f3"),
        "the ledger must pin the reference version and revision it was measured against"
    );

    // `docs/api.md` §3.5 keeps its residual prose and points here.
    let api = std::fs::read_to_string(API_MD).expect("read docs/api.md");
    assert!(
        api.contains("metrics-differential-ledger.md"),
        "docs/api.md §3.5 must point at the ledger the row moved into"
    );
}

/// The retired id must appear nowhere in the tree — two ids for one
/// divergence is how a row goes stale while a gate stays green.
#[test]
fn the_retired_reject_status_id_appears_nowhere() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut hits: Vec<String> = Vec::new();
    scan_for(&root, &retired_ledger_id(), &mut hits);
    assert!(
        hits.is_empty(),
        "retired ledger id still present in: {hits:?}"
    );
}

fn scan_for(dir: &Path, needle: &str, hits: &mut Vec<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name == ".git" || name == "target" || name == "vendor" || name == "node_modules" {
            continue;
        }
        if path.is_dir() {
            scan_for(&path, needle, hits);
        } else if let Ok(text) = std::fs::read_to_string(&path)
            && text.contains(needle)
        {
            hits.push(path.display().to_string());
        }
    }
}
