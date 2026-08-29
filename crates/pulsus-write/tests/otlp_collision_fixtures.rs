//! Issue #461 AC20: the float / native-histogram collision, fixed at every
//! identity the reference can produce it at, in **both** arrival orders.
//!
//! Prometheus v3.13.0's OTLP translator has thirteen `appender.Append` call
//! sites; two pass a histogram (`histograms.go:79`, and `:300` behind the
//! off-by-default NHCB gate), so **eleven append a float**. Each of those
//! eleven produces a distinct translated series identity, and each of those
//! identities can be collided by an OTLP exponential histogram that
//! translates to the same name and label set at the same timestamp. This
//! file carries one fixture per site, run float-first and histogram-first,
//! plus the three negative controls that break one component each of the
//! rule "collision is decided by the complete translated series identity —
//! name plus labels — together with the timestamp".
//!
//! Every `reference` half in
//! `tests/fixtures/otlp-metrics/prom-translation/collisions.json` was
//! **captured from a running `prom/prometheus:v3.13.0`** by
//! [`capture_collisions_json`] below, never typed by hand:
//!
//! ```text
//! PULSUS_CAPTURE_PROM_COLLISIONS=127.0.0.1:39470 \
//!   cargo test -p pulsus-write --test otlp_collision_fixtures -- --ignored --nocapture
//! ```
//!
//! The container must be **fresh** and started with
//! `--web.enable-otlp-receiver --enable-feature=native-histograms`; without
//! the second flag an OTLP exponential histogram never becomes a stored
//! histogram and no fixture here can collide. As in the sibling corpus, the
//! reference's admission window is head-relative, so the capture restamps
//! every push to run time and rewrites the committed payloads back to
//! [`REFERENCE_TS_MS`] afterwards — including inside the captured bodies,
//! which quote the refused sample's own timestamp.
//!
//! **Why both orders.** The reference keeps the first arrival, so the two
//! orders give different bodies: the generic `duplicate sample for
//! timestamp\n` when the float landed first, and the detailed `duplicate
//! sample for timestamp <ts>; overrides not allowed: existing is a
//! histogram, new value <v>\n` when the histogram did. Neither `<ts>` nor
//! `<v>` is pinned here: the runner derives both from the case's own push
//! and asserts the captured body equals what that derivation composes.
//! PulsusDB's answer is the same in every row and every order — the
//! histogram wins, deterministically.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use pulsus_config::{ExpHistogramMode, OtlpTranslationStrategy};
use pulsus_write::ParsedMetrics;
use pulsus_write::protocols::otlp_metrics::{self, MetricIngestSettings};
use serde_json::{Value, json};

/// The fixed `timeUnixNano` every committed payload carries.
const REFERENCE_TS_MS: i64 = 1_787_920_000_000;

const CASES_JSON: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/otlp-metrics/prom-translation/collisions.json"
);

/// The eleven float `appender.Append` sites of plan v13 Δ44, enumerated
/// from the reference's own source rather than from shapes we thought of,
/// each with the fixture that collides the identity it produces.
///
/// Sites 10 and 11 (`helper.go:586` and `:603`) produce **one** identity at
/// two timestamps within a resource's span, so `collision-target-info`
/// covers both; the single-point fixture exercises the final-sample site.
const COLLISION_FIXTURES: &[(&str, &str)] = &[
    ("collision-gauge-base-name", "number_data_points.go:66"),
    ("collision-sum-base-name", "number_data_points.go:116"),
    (
        "collision-monotonic-sum-already-total",
        "number_data_points.go:116",
    ),
    ("collision-classic-histogram-sum", "helper.go:250"),
    ("collision-classic-histogram-count", "helper.go:261"),
    ("collision-classic-histogram-bucket-bound", "helper.go:299"),
    ("collision-classic-histogram-bucket-inf", "helper.go:311"),
    ("collision-summary-sum", "helper.go:450"),
    ("collision-summary-count", "helper.go:460"),
    ("collision-summary-generated-quantile", "helper.go:473"),
    ("collision-target-info", "helper.go:586 + helper.go:603"),
];

/// One control per component of the identity rule (plan v13 Δ45), each run
/// in **both** orders (plan v14 Δ48): a control that runs one way proves
/// the absence of a collision for one arrival order only, which is exactly
/// the asymmetry the collision fixtures exist to capture.
const CONTROL_FIXTURES: &[(&str, &str)] = &[
    ("control-label-inequality", "label equality"),
    ("control-timestamp-inequality", "timestamp equality"),
    ("control-name-inequality", "name equality"),
];

const DIRECTIONS: &[&str] = &["float-first", "histogram-first"];

/// Plan v15 Δ52, printed whenever a reference answer disagrees with the
/// signature its declaration implies. A fixture whose two pushes carry
/// different timestamps when they should share one is **silently green
/// forward with an empty body** and **red reversed with `out of order
/// sample`** — never with a collision body. Without this table a reversed-
/// only red reads as a collision finding, which it is not.
const MIS_BUILT_SIGNATURE: &str = "\
plan v15 Δ52 — how to read this failure:

  mis-built (the two pushes carry DIFFERENT timestamps)
    forward  float@T1 then hist@T2 : 200 then 200  ''                        readback = histogram
    reverse  hist@T2 then float@T1 : 200 then 400  'out of order sample\\n'   readback = histogram

  correctly built (one shared timestamp)
    forward  float@T1 then hist@T1 : 200 then 400  'duplicate sample for timestamp\\n'
    reverse  hist@T1 then float@T1 : 200 then 400  'duplicate sample for timestamp <ts>; overrides
                                                    not allowed: existing is a histogram, new value <v>\\n'

A pair that is green forward and `out of order sample` reversed is a
MIS-BUILT FIXTURE, not a collision finding.";

// ---------------------------------------------------------------------
// Corpus model

fn load_cases() -> Value {
    let raw =
        std::fs::read_to_string(CASES_JSON).unwrap_or_else(|e| panic!("read {CASES_JSON}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {CASES_JSON}: {e}"))
}

fn cases(doc: &Value) -> &Vec<Value> {
    doc["cases"].as_array().expect("cases")
}

fn id_of(case: &Value) -> &str {
    case["id"].as_str().expect("id")
}

fn settings_of(case: &Value) -> MetricIngestSettings {
    MetricIngestSettings {
        // AC20: every fixture here is a `native`-mode case (plan v13, and
        // restored to cover the controls by v27). `to_native_histogram` has
        // one production call site, inside `emit_native_exponential_histogram`,
        // which runs only under `native`/`dual`; under the default `classic`
        // mode an OTLP exponential histogram is flattened into `_bucket`
        // floats and **no fixture in this file can collide at all**.
        exp_histogram_mode: match case["exp_histogram_mode"].as_str().expect("mode") {
            "native" => ExpHistogramMode::Native,
            other => panic!("case {} declares mode {other:?}", id_of(case)),
        },
        translation_strategy: case["strategy"]
            .as_str()
            .expect("strategy")
            .parse::<OtlpTranslationStrategy>()
            .expect("one of the four reference spellings"),
        promote_scope_metadata: case["promote_scope_metadata"]
            .as_bool()
            .expect("promote_scope_metadata"),
        promql_lookback_ms: 300_000,
    }
}

fn pushes(case: &Value) -> &Vec<Value> {
    case["pushes"].as_array().expect("pushes")
}

/// Decodes one push's payload **through the production decoder**, from the
/// exact bytes the capture posts.
fn decode(push: &Value) -> ExportMetricsServiceRequest {
    let body = serde_json::to_vec(&push["payload"]).expect("payload re-serializes");
    otlp_metrics::decode_json(&body).expect("collision payloads are valid OTLP/JSON")
}

/// The metric names, attribute keys and data-kind carried by a decoded
/// request — read back out of the decoded protobuf, never out of the JSON.
fn wire_of(request: &ExportMetricsServiceRequest) -> (Vec<String>, Vec<String>, Vec<&'static str>) {
    let mut names = Vec::new();
    let mut keys: BTreeSet<String> = BTreeSet::new();
    let mut kinds = Vec::new();
    for rm in &request.resource_metrics {
        if let Some(resource) = &rm.resource {
            keys.extend(resource.attributes.iter().map(|kv| kv.key.clone()));
        }
        for sm in &rm.scope_metrics {
            if let Some(scope) = &sm.scope {
                keys.extend(scope.attributes.iter().map(|kv| kv.key.clone()));
            }
            for metric in &sm.metrics {
                names.push(metric.name.clone());
                let (kind, attrs): (&'static str, Vec<Vec<_>>) = match &metric.data {
                    Some(Data::Gauge(g)) => (
                        "gauge",
                        g.data_points.iter().map(|d| d.attributes.clone()).collect(),
                    ),
                    Some(Data::Sum(s)) => (
                        "sum",
                        s.data_points.iter().map(|d| d.attributes.clone()).collect(),
                    ),
                    Some(Data::Histogram(h)) => (
                        "histogram",
                        h.data_points.iter().map(|d| d.attributes.clone()).collect(),
                    ),
                    Some(Data::ExponentialHistogram(e)) => (
                        "exponentialHistogram",
                        e.data_points.iter().map(|d| d.attributes.clone()).collect(),
                    ),
                    Some(Data::Summary(s)) => (
                        "summary",
                        s.data_points.iter().map(|d| d.attributes.clone()).collect(),
                    ),
                    None => panic!("a metric with no data oneof"),
                };
                kinds.push(kind);
                for set in attrs {
                    keys.extend(set.into_iter().map(|kv| kv.key));
                }
            }
        }
    }
    (names, keys.into_iter().collect(), kinds)
}

/// Every `timeUnixNano` a decoded request carries, in milliseconds.
fn timestamps_ms(request: &ExportMetricsServiceRequest) -> BTreeSet<i64> {
    let mut out = BTreeSet::new();
    let mut add = |ns: u64| {
        out.insert((ns / 1_000_000) as i64);
    };
    for rm in &request.resource_metrics {
        for sm in &rm.scope_metrics {
            for metric in &sm.metrics {
                match &metric.data {
                    Some(Data::Gauge(g)) => {
                        g.data_points.iter().for_each(|d| add(d.time_unix_nano))
                    }
                    Some(Data::Sum(s)) => s.data_points.iter().for_each(|d| add(d.time_unix_nano)),
                    Some(Data::Histogram(h)) => {
                        h.data_points.iter().for_each(|d| add(d.time_unix_nano))
                    }
                    Some(Data::ExponentialHistogram(e)) => {
                        e.data_points.iter().for_each(|d| add(d.time_unix_nano))
                    }
                    Some(Data::Summary(s)) => {
                        s.data_points.iter().for_each(|d| add(d.time_unix_nano))
                    }
                    None => {}
                }
            }
        }
    }
    out
}

fn parse_push(case: &Value, push: &Value) -> ParsedMetrics {
    otlp_metrics::parse(
        &decode(push),
        REFERENCE_TS_MS * 1_000_000,
        settings_of(case),
    )
    .unwrap_or_else(|e| panic!("case {}: parse failed: {e}", id_of(case)))
}

/// The two pushes concatenated into **one** request in the case's own
/// order: the shape our stateless writer can resolve, and the one
/// `dedup_histogram_wins` runs over.
fn merged_request(case: &Value) -> ExportMetricsServiceRequest {
    let mut merged = ExportMetricsServiceRequest {
        resource_metrics: Vec::new(),
    };
    for push in pushes(case) {
        merged
            .resource_metrics
            .extend(decode(push).resource_metrics);
    }
    merged
}

fn push_with_role<'a>(case: &'a Value, role: &str) -> &'a Value {
    pushes(case)
        .iter()
        .find(|p| p["role"] == json!(role))
        .unwrap_or_else(|| panic!("case {} has no {role} push", id_of(case)))
}

/// `(metric_name, fingerprint)` — the translated series identity, WITHOUT
/// the timestamp, so the timestamp clause of the rule can be varied
/// independently of it.
type Identity = (String, u64);

fn float_identities(parsed: &ParsedMetrics) -> BTreeMap<Identity, f64> {
    parsed
        .samples
        .iter()
        .map(|s| ((s.metric_name.to_string(), s.fingerprint), s.value))
        .collect()
}

fn hist_identities(parsed: &ParsedMetrics) -> BTreeSet<Identity> {
    parsed
        .hist_samples
        .iter()
        .map(|h| (h.metric_name.to_string(), h.fingerprint))
        .collect()
}

/// The identities on which this case's float push and histogram push agree
/// — the collision, when there is one.
fn shared_identities(case: &Value) -> Vec<(Identity, f64)> {
    let floats = float_identities(&parse_push(case, push_with_role(case, "float")));
    let hists = hist_identities(&parse_push(case, push_with_role(case, "histogram")));
    floats
        .into_iter()
        .filter(|(identity, _)| hists.contains(identity))
        .collect()
}

// ---------------------------------------------------------------------
// AC20 — the roster

/// The fixture set is exactly the eleven float append sites and the three
/// negative controls, each in **both** directions.
///
/// Stated as an equality rather than a containment: a fixture the roster
/// does not name fails just as a named fixture that is missing does, so the
/// file cannot grow a case nobody enumerated or lose one silently.
#[test]
fn the_roster_is_the_eleven_float_append_sites_and_three_controls_in_both_orders() {
    let doc = load_cases();
    let mut expected: BTreeSet<String> = BTreeSet::new();
    for (fixture, _) in COLLISION_FIXTURES.iter().chain(CONTROL_FIXTURES) {
        for direction in DIRECTIONS {
            expected.insert(format!("{fixture}:{direction}"));
        }
    }
    let found: BTreeSet<String> = cases(&doc).iter().map(|c| id_of(c).to_string()).collect();
    assert_eq!(found, expected);
    assert_eq!(
        expected.len(),
        (11 + 3) * 2,
        "eleven float append sites and three controls, both orders each"
    );

    for case in cases(&doc) {
        let id = id_of(case);
        assert_eq!(
            id,
            format!(
                "{}:{}",
                case["fixture"].as_str().expect("fixture"),
                case["direction"].as_str().expect("direction")
            ),
            "a case's id must name its own fixture and direction"
        );
        assert_eq!(pushes(case).len(), 2, "case {id}: a fixture is two pushes");
        let roles: Vec<&str> = pushes(case)
            .iter()
            .map(|p| p["role"].as_str().expect("role"))
            .collect();
        let expected_roles = match case["direction"].as_str().expect("direction") {
            "float-first" => ["float", "histogram"],
            "histogram-first" => ["histogram", "float"],
            other => panic!("case {id}: unknown direction {other:?}"),
        };
        assert_eq!(
            roles, expected_roles,
            "case {id}: push roles follow the direction"
        );
    }
}

/// AC20: **all** fixtures here are `native`-mode cases — the eleven
/// collision fixtures and the three negative controls alike (plan v13, and
/// restored to the controls by v27).
///
/// The controls need it as much as the collisions do: under `classic` an
/// exponential histogram produces float `_bucket` series instead of a
/// native histogram, so `control-timestamp-inequality` would stop being a
/// near-miss of a collision and start being two unrelated float series,
/// and its `200`/`200` would mean nothing.
#[test]
fn every_fixture_is_a_native_mode_case() {
    let doc = load_cases();
    for case in cases(&doc) {
        assert_eq!(
            case["exp_histogram_mode"],
            json!("native"),
            "case {}: AC20 fixtures are native-mode cases",
            id_of(case)
        );
        // `settings_of` panics on anything else; call it so the declaration
        // is not merely a string in a file nobody reads.
        assert_eq!(
            settings_of(case).exp_histogram_mode,
            ExpHistogramMode::Native
        );
    }
}

// ---------------------------------------------------------------------
// AC20 — the wire check (plan v13 Δ46)

/// Each case **records** the metric names and attribute keys as decoded
/// from its own payload bytes, and this asserts they match the case's
/// **declared inputs**.
///
/// The near-miss that produced the requirement was a harness that appended
/// a run marker to the metric name, so the case being tested did not exist
/// and the result agreed with nothing. Three links are closed here: the
/// declared input (what the fixture claims to push), the recorded wire
/// (what the generator decoded), and a fresh decode of the committed bytes
/// by the production decoder. Editing the payload without editing the
/// declaration, or the reverse, reddens.
#[test]
fn every_push_declares_the_names_and_label_keys_on_its_own_wire() {
    let doc = load_cases();
    for case in cases(&doc) {
        let id = id_of(case);
        for push in pushes(case) {
            let role = push["role"].as_str().expect("role");
            let (names, keys, kinds) = wire_of(&decode(push));

            let recorded_names: Vec<String> = push["wire"]["metric_names"]
                .as_array()
                .expect("wire.metric_names")
                .iter()
                .map(|v| v.as_str().expect("name").to_string())
                .collect();
            let recorded_keys: Vec<String> = push["wire"]["attribute_keys"]
                .as_array()
                .expect("wire.attribute_keys")
                .iter()
                .map(|v| v.as_str().expect("key").to_string())
                .collect();
            assert_eq!(
                names, recorded_names,
                "case {id} / {role}: the recorded wire names are not what the payload decodes to"
            );
            assert_eq!(
                keys, recorded_keys,
                "case {id} / {role}: the recorded wire keys are not what the payload decodes to"
            );

            let declared = &case["inputs"][role];
            assert_eq!(
                names,
                vec![
                    declared["metric_name"]
                        .as_str()
                        .expect("inputs.metric_name")
                        .to_string()
                ],
                "case {id} / {role}: the wire name is not the declared input"
            );
            assert_eq!(
                kinds,
                vec![declared["kind"].as_str().expect("inputs.kind")],
                "case {id} / {role}: the wire data kind is not the declared input"
            );
            assert_eq!(
                recorded_keys,
                declared["attribute_keys"]
                    .as_array()
                    .expect("inputs.attribute_keys")
                    .iter()
                    .map(|v| v.as_str().expect("key").to_string())
                    .collect::<Vec<_>>(),
                "case {id} / {role}: the wire keys are not the declared inputs"
            );
        }
    }
}

// ---------------------------------------------------------------------
// AC20 — the timestamp rule (plan v15 Δ51)

/// The timestamp rule has one clause per case shape, and both halves are
/// checked: the declaration is **self-consistent**, and the payloads
/// **agree with it**.
///
/// - two pushes on the same identity, testing the collision: they share one
///   timestamp, because equality is what is under test;
/// - two pushes on different identities: they share one timestamp too —
///   ordering is enforced per series, so direction does not matter;
/// - two pushes on the same identity testing timestamp inequality: the
///   **second** push carries the later stamp, in whichever direction the
///   case runs.
///
/// "Every second push carries the later one" was only ever true of the
/// third bullet, and a fixture built that way is the mis-built pair of
/// [`MIS_BUILT_SIGNATURE`].
#[test]
fn the_timestamp_declaration_is_self_consistent_and_matches_the_payloads() {
    let doc = load_cases();
    let mut relations: BTreeSet<&str> = BTreeSet::new();
    for case in cases(&doc) {
        let id = id_of(case);
        let same_identity = case["same_identity"].as_bool().expect("same_identity");
        let relation = case["timestamp_relation"]
            .as_str()
            .expect("timestamp_relation");
        relations.insert(relation);

        // Self-consistency, structural: a cross-identity case has no
        // ordering hazard to express, so it shares one timestamp.
        if relation == "second-later" {
            assert!(
                same_identity,
                "case {id}: only a same-identity case can test timestamp inequality"
            );
        }
        // ...and a case that is testing the collision must declare equal
        // timestamps, or it is not testing the collision.
        let is_control = CONTROL_FIXTURES
            .iter()
            .any(|(f, _)| case["fixture"] == json!(*f));
        if !is_control {
            assert!(
                same_identity && relation == "equal",
                "case {id}: a collision fixture declares one shared timestamp on one identity"
            );
        }

        // The payloads, read back from their own bytes.
        let stamps: Vec<BTreeSet<i64>> = pushes(case)
            .iter()
            .map(|p| timestamps_ms(&decode(p)))
            .collect();
        for (i, set) in stamps.iter().enumerate() {
            assert_eq!(
                set.len(),
                1,
                "case {id}: push {i} must carry exactly one timestamp"
            );
        }
        let first = *stamps[0].iter().next().expect("one timestamp");
        let second = *stamps[1].iter().next().expect("one timestamp");
        match relation {
            "equal" => assert_eq!(
                first, second,
                "case {id} declares equal timestamps, but its payloads carry {first} and \
                 {second}\n\n{MIS_BUILT_SIGNATURE}"
            ),
            "second-later" => assert!(
                second > first,
                "case {id} declares that the SECOND push carries the later stamp, but its \
                 payloads carry {first} then {second}\n\n{MIS_BUILT_SIGNATURE}"
            ),
            other => panic!("case {id}: unknown timestamp_relation {other:?}"),
        }
    }
    assert_eq!(
        relations,
        BTreeSet::from(["equal", "second-later"]),
        "both timestamp relations must be exercised, or the rule has only one branch"
    );
}

// ---------------------------------------------------------------------
// AC20 — the reference's answers, per direction

/// The body the reference writes when the **float** lands second, composed
/// from the case's own push rather than pinned: `<ts>` is the timestamp the
/// payload carries and `<v>` is the value our own translation emits for the
/// colliding identity.
///
/// Rendering `<v>` with Rust's shortest round-trip `{}` matches Go's `%v`
/// over the values these fixtures use (`7`, `2`, `1.5`, `0.25`, `1`); the
/// two formats can differ for very large or very small magnitudes, which no
/// fixture here has.
fn float_second_body(ts_ms: i64, value: f64) -> String {
    format!(
        "duplicate sample for timestamp {ts_ms}; overrides not allowed: existing is a histogram, \
         new value {value}\n"
    )
}

/// The captured reference status and body for each push match the signature
/// the case's declaration implies.
///
/// A declared collision (same identity, one shared timestamp) is `200` then
/// `400`, and the `400`'s body names the direction. Anything else — a
/// broken label, name or timestamp — is `200`/`200` with no body at all, in
/// **both** directions.
#[test]
fn the_reference_answers_match_the_signature_each_declaration_implies() {
    let doc = load_cases();
    let mut collisions = 0usize;
    let mut controls = 0usize;
    for case in cases(&doc) {
        let id = id_of(case);
        let same_identity = case["same_identity"].as_bool().expect("same_identity");
        let relation = case["timestamp_relation"]
            .as_str()
            .expect("timestamp_relation");
        let expects_collision = same_identity && relation == "equal";

        let observed: Vec<(i64, Option<&str>, &str)> = pushes(case)
            .iter()
            .map(|p| {
                let r = &p["reference"];
                (
                    r["status"].as_i64().expect("captured status"),
                    r["content_type"].as_str(),
                    r["body"].as_str().expect("captured body"),
                )
            })
            .collect();

        if !expects_collision {
            controls += 1;
            assert_eq!(
                (observed[0].0, observed[0].2, observed[1].0, observed[1].2),
                (200, "", 200, ""),
                "case {id} declares no collision, so the reference must answer 200/200 with no \
                 body, and it answered {observed:?}\n\n{MIS_BUILT_SIGNATURE}"
            );
            assert!(
                observed.iter().all(|o| o.1.is_none()),
                "case {id}: the reference's 200 carries no Content-Type"
            );
            continue;
        }

        collisions += 1;
        assert_eq!(
            observed[0].0, 200,
            "case {id}: the first arrival is always accepted\n\n{MIS_BUILT_SIGNATURE}"
        );
        assert_eq!(
            observed[0].2, "",
            "case {id}: the reference's 200 has no body"
        );
        assert_eq!(
            observed[1].0, 400,
            "case {id}: the second arrival collides\n\n{MIS_BUILT_SIGNATURE}"
        );
        assert_eq!(
            observed[1].1,
            Some("text/plain; charset=utf-8"),
            "case {id}: a reference rejection is plain text, never an OTLP envelope"
        );

        // The body, per direction, derived rather than pinned.
        let second = &pushes(case)[1];
        let expected_body = match second["role"].as_str().expect("role") {
            "histogram" => "duplicate sample for timestamp\n".to_string(),
            "float" => {
                let shared = shared_identities(case);
                assert_eq!(
                    shared.len(),
                    1,
                    "case {id}: exactly one identity must collide, found {shared:?}"
                );
                let ts = *timestamps_ms(&decode(second))
                    .iter()
                    .next()
                    .expect("one timestamp");
                float_second_body(ts, shared[0].1)
            }
            other => panic!("case {id}: unknown role {other:?}"),
        };
        assert_eq!(
            observed[1].2, expected_body,
            "case {id}: the captured body is not what this push's own timestamp and value \
             compose\n\n{MIS_BUILT_SIGNATURE}"
        );
    }
    assert_eq!(collisions, 22, "eleven collision fixtures, both orders");
    assert_eq!(controls, 6, "three negative controls, both orders");
}

// ---------------------------------------------------------------------
// AC20 — our own answer

/// Every case's declared identity relation is the one **our** translation
/// produces, so no fixture can be vacuous.
///
/// This is what stops a control from passing because it failed to build a
/// near-collision at all: `control-label-inequality` withholds exactly one
/// generated label from a pair that otherwise collides, and the collision
/// fixture beside it proves that pair can collide. The two controls that
/// break name and label equality must share **no** identity; the one that
/// breaks timestamp equality must share **one**, since only the timestamps
/// differ.
#[test]
fn our_translation_produces_the_identity_relation_each_case_declares() {
    let doc = load_cases();
    for case in cases(&doc) {
        let id = id_of(case);
        let same_identity = case["same_identity"].as_bool().expect("same_identity");
        let shared = shared_identities(case);
        if same_identity {
            assert_eq!(
                shared.len(),
                1,
                "case {id} declares one shared identity; our translation produced {shared:?}"
            );
        } else {
            assert!(
                shared.is_empty(),
                "case {id} declares no shared identity; our translation produced {shared:?}"
            );
            // ...and the two pushes must still each produce something, or
            // "they share nothing" is true for the wrong reason.
            for role in ["float", "histogram"] {
                let parsed = parse_push(case, push_with_role(case, role));
                assert!(
                    !parsed.samples.is_empty() || !parsed.hist_samples.is_empty(),
                    "case {id} / {role}: the push emitted no series at all, so the control's \
                     absence check is vacuous"
                );
            }
        }
    }
}

/// **The histogram wins, deterministically, in every order.**
///
/// A stateless writer cannot resolve a collision spread across two
/// requests — that is the read path's tie-break — so the seam this asserts
/// is the one our writer owns: the same two metrics arriving in **one**
/// request, concatenated in the case's own order. `dedup_histogram_wins`
/// drops the float at the colliding `(name, fingerprint, unix_milli)` and
/// keeps the histogram, whichever arrived first.
///
/// The controls are the non-vacuous half: they run the identical merge and
/// must drop **nothing**, so a dedup that keyed on the name alone, or
/// ignored the timestamp, fails `control-label-inequality` and
/// `control-timestamp-inequality` respectively while still passing every
/// collision fixture.
#[test]
fn the_histogram_wins_in_both_orders_and_the_two_orders_agree() {
    let doc = load_cases();
    let mut by_fixture: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();

    for case in cases(&doc) {
        let id = id_of(case);
        let same_identity = case["same_identity"].as_bool().expect("same_identity");
        let relation = case["timestamp_relation"]
            .as_str()
            .expect("timestamp_relation");
        let expects_collision = same_identity && relation == "equal";

        let parsed = otlp_metrics::parse(
            &merged_request(case),
            REFERENCE_TS_MS * 1_000_000,
            settings_of(case),
        )
        .unwrap_or_else(|e| panic!("case {id}: merged parse failed: {e}"));

        if expects_collision {
            let shared = shared_identities(case);
            assert_eq!(shared.len(), 1, "case {id}: one identity must collide");
            let identity = &shared[0].0;
            assert_eq!(
                parsed.rejected, 1,
                "case {id}: exactly one float sample is dropped, got {}",
                parsed.rejected
            );
            assert_eq!(
                parsed.rejected_message.as_deref(),
                Some(
                    "float sample dropped: native histogram present at the same series and \
                     timestamp"
                ),
                "case {id}: the drop must be reported, never swallowed"
            );
            assert!(
                !float_identities(&parsed).contains_key(identity),
                "case {id}: the colliding float survived at {identity:?}"
            );
            assert!(
                hist_identities(&parsed).contains(identity),
                "case {id}: the histogram must be the survivor at {identity:?}"
            );
        } else {
            assert_eq!(
                parsed.rejected, 0,
                "case {id}: a control must drop nothing, but {} sample(s) were dropped ({:?})",
                parsed.rejected, parsed.rejected_message
            );
            assert!(
                !parsed.samples.is_empty(),
                "case {id}: the control must still emit float samples"
            );
            assert!(
                !parsed.hist_samples.is_empty(),
                "case {id}: the control must still emit a native histogram"
            );
        }

        // A stable rendering of what the merged request leaves behind, so
        // the two directions can be compared to each other.
        let mut floats: Vec<String> = parsed
            .samples
            .iter()
            .map(|s| {
                format!(
                    "{} #{} @{} = {}",
                    s.metric_name, s.fingerprint, s.unix_milli, s.value
                )
            })
            .collect();
        floats.sort();
        let mut hists: Vec<String> = parsed
            .hist_samples
            .iter()
            .map(|h| format!("{} #{} @{}", h.metric_name, h.fingerprint, h.unix_milli))
            .collect();
        hists.sort();
        // Only a fixture whose two pushes share ONE timestamp can be
        // compared across its directions. `control-timestamp-inequality`
        // is excluded by construction, not by convenience: its later stamp
        // is a property of the push POSITION, so swapping the pushes also
        // swaps which metric carries it, and the two directions store
        // different rows for a reason that has nothing to do with the
        // collision rule (plan v14 Δ49).
        if relation == "equal" {
            by_fixture
                .entry(case["fixture"].as_str().expect("fixture").to_string())
                .or_default()
                .push((
                    case["direction"].as_str().expect("direction").to_string(),
                    format!("floats={floats:#?}\nhists={hists:#?}"),
                ));
        }
    }

    // Deterministic means order-independent: both directions of a fixture
    // leave the identical rows. The two directions of a fixture share one
    // `service.name`, so their identities are the same and this comparison
    // is meaningful rather than trivially unequal.
    assert_eq!(
        by_fixture.len(),
        13,
        "eleven collision fixtures and the two shared-timestamp controls"
    );
    for (fixture, mut runs) in by_fixture {
        runs.sort();
        assert_eq!(runs.len(), 2, "fixture {fixture}: both directions");
        assert_eq!(
            runs[0].1, runs[1].1,
            "fixture {fixture}: {} and {} left different rows behind, so the resolution is \
             order-dependent",
            runs[0].0, runs[1].0
        );
    }
}

// ---------------------------------------------------------------------
// The capture harness (`#[ignore]`)

/// Minimal HTTP/1.1 over a bare `TcpStream`, the idiom this repo already
/// uses for live suites.
struct HttpResponse {
    status: u16,
    content_type: Option<String>,
    body: String,
}

fn http(endpoint: &str, method: &str, path: &str, body: Option<&[u8]>) -> HttpResponse {
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
    let content_type = head
        .lines()
        .find(|line| {
            line.split(':')
                .next()
                .is_some_and(|name| name.eq_ignore_ascii_case("content-type"))
        })
        .and_then(|line| line.split_once(':'))
        .map(|(_, value)| value.trim().to_string());
    HttpResponse {
        status,
        content_type,
        body: String::from_utf8_lossy(&raw[split + 4..]).to_string(),
    }
}

/// Shifts every `timeUnixNano` by `delta_ns`, preserving the relation
/// between the two pushes of a case.
fn shift(value: &mut Value, delta_ns: i64) {
    match value {
        Value::Object(map) => {
            for (key, entry) in map.iter_mut() {
                if key == "timeUnixNano" {
                    let ns: i64 = entry
                        .as_str()
                        .expect("timeUnixNano is a string")
                        .parse()
                        .expect("timeUnixNano parses");
                    *entry = json!((ns + delta_ns).to_string());
                } else {
                    shift(entry, delta_ns);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(|v| shift(v, delta_ns)),
        _ => {}
    }
}

/// Regenerates the `reference` half of every push from a live
/// `prom/prometheus:v3.13.0`. Ignored by default: it needs a running
/// container and it rewrites a committed fixture.
#[test]
#[ignore = "captures expectations from a live prom/prometheus:v3.13.0 container"]
fn capture_collisions_json() {
    let endpoint = std::env::var("PULSUS_CAPTURE_PROM_COLLISIONS")
        .expect("set PULSUS_CAPTURE_PROM_COLLISIONS=host:port");
    let mut doc = load_cases();
    let now_s = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs() as i64;
    // Inside the reference's 60-minute head-relative admission window, with
    // room for the `second-later` cases' extra second.
    let anchor_s = now_s - 900;

    let info = http(&endpoint, "GET", "/api/v1/status/buildinfo", None);
    let build_info: Value = serde_json::from_str(&info.body).expect("buildinfo");

    let cases = doc["cases"].as_array_mut().expect("cases");
    for (index, case) in cases.iter_mut().enumerate() {
        let id = case["id"].as_str().expect("id").to_string();
        let delta_ns = (anchor_s + index as i64 * 3) * 1_000_000_000 - REFERENCE_TS_MS * 1_000_000;
        for push in case["pushes"].as_array_mut().expect("pushes") {
            let mut payload = push["payload"].clone();
            shift(&mut payload, delta_ns);
            let committed_ms = {
                let body = serde_json::to_vec(&push["payload"]).expect("payload");
                *timestamps_ms(&otlp_metrics::decode_json(&body).expect("decode"))
                    .iter()
                    .next()
                    .expect("one timestamp")
            };
            let run_ms = committed_ms + delta_ns / 1_000_000;

            let body = serde_json::to_vec(&payload).expect("payload");
            let answer = http(&endpoint, "POST", "/api/v1/otlp/v1/metrics", Some(&body));
            // The reference quotes the refused sample's own timestamp, so
            // the captured body has to come back to the committed anchor
            // exactly as the payload does.
            let normalized = answer
                .body
                .replace(&run_ms.to_string(), &committed_ms.to_string());
            println!(
                "{id} / {} @{run_ms} -> {} {:?}",
                push["role"], answer.status, normalized
            );
            push["reference"] = json!({
                "status": answer.status,
                "content_type": answer.content_type,
                "body": normalized,
            });
        }
    }

    doc["captured_from"] = json!({
        "image": "docker.io/prom/prometheus:v3.13.0",
        "buildinfo": build_info["data"],
        "flags": "--web.enable-otlp-receiver --enable-feature=native-histograms",
        "route": "POST /api/v1/otlp/v1/metrics",
        "note": "each push restamped to run time; captured bodies rewritten back to \
                 reference_timestamp_ms",
    });

    let rendered = format!("{}\n", serde_json::to_string_pretty(&doc).expect("render"));
    std::fs::write(CASES_JSON, rendered).unwrap_or_else(|e| panic!("write {CASES_JSON}: {e}"));
    println!(
        "captured {} cases into {CASES_JSON}",
        doc["cases"].as_array().unwrap().len()
    );
}
