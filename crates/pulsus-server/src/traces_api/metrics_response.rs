//! The Tempo-native TraceQL metrics response body (issue #182) — the wire
//! shape the Tempo datasource expects from
//! `/api/traces/v1/metrics/{query_range,query}` and the two datasource
//! aliases. This **replaces** the Prometheus matrix/vector envelope on
//! those endpoints (a documented breaking change; they are
//! Tempo-datasource-only and never spoke PromQL).
//!
//! Clean-room: the shape below was authored from the published
//! grafana.com/docs/tempo docs plus a black-box capture of the pinned
//! `grafana/tempo:3.0.2` container (Plan v3 Fix 1) — no Tempo/`tempopb`
//! source, `.proto`, or generated code was read or vendored. The captured
//! invariants, all pinned by the byte-for-byte encoder golden below:
//!   * top level `{"series":[…],"metrics":{"completedJobs":…,"totalJobs":…}}`
//!   * labels are OTLP protojson `AnyValue` (camelCase `stringValue`/
//!     `doubleValue`)
//!   * `timestampMs` is a JSON **string** int64
//!   * a sample `value` is **omitted when zero** (protojson default omission)
//!   * exemplars carry the trace reference as a `trace:id` label, not a
//!     top-level `traceId`/`spanId`

use axum::response::{IntoResponse, Response};
use serde::{Serialize, Serializer};

use pulsus_read::{MetricExemplar, MetricLabel, MetricLabelValue, TraceMetricsResult};

/// Serializes an `i64` as a JSON string (protojson int64 convention).
fn i64_str<S: Serializer>(v: &i64, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&v.to_string())
}

/// Matches Tempo's protojson default-omission of a zero `value`.
fn f64_is_zero(v: &f64) -> bool {
    *v == 0.0
}

#[derive(Serialize)]
struct MetricsResponse {
    series: Vec<TsSeries>,
    metrics: RangeMetrics,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RangeMetrics {
    completed_jobs: u32,
    total_jobs: u32,
}

#[derive(Serialize)]
struct TsSeries {
    labels: Vec<Label>,
    samples: Vec<Sample>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    exemplars: Vec<Exemplar>,
}

#[derive(Serialize)]
struct Label {
    key: String,
    value: AnyValue,
}

/// The OTLP protojson `AnyValue` subset Tempo emits for metric labels.
/// Externally tagged with camelCase field names → `{"stringValue":…}` /
/// `{"doubleValue":…}`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
enum AnyValue {
    StringValue(String),
    DoubleValue(f64),
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Sample {
    #[serde(serialize_with = "i64_str")]
    timestamp_ms: i64,
    #[serde(skip_serializing_if = "f64_is_zero")]
    value: f64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Exemplar {
    labels: Vec<Label>,
    value: f64,
    #[serde(serialize_with = "i64_str")]
    timestamp_ms: i64,
}

fn label(l: &MetricLabel) -> Label {
    let value = match &l.value {
        MetricLabelValue::Str(s) => AnyValue::StringValue(s.clone()),
        MetricLabelValue::Double(d) => AnyValue::DoubleValue(*d),
    };
    Label {
        key: l.key.clone(),
        value,
    }
}

fn exemplar(e: &MetricExemplar) -> Exemplar {
    Exemplar {
        labels: e.labels.iter().map(label).collect(),
        value: e.value,
        timestamp_ms: e.timestamp_ms,
    }
}

/// Frames the engine result into the Tempo-native response body. `metrics`
/// reports a single synchronous job (`completedJobs == totalJobs == 1`) —
/// the reader executes one pushed-down query, so progress is always 100%.
fn build(result: &TraceMetricsResult) -> MetricsResponse {
    let series = result
        .series
        .iter()
        .map(|s| TsSeries {
            labels: s.labels.iter().map(label).collect(),
            samples: s
                .samples
                .iter()
                .map(|&(timestamp_ms, value)| Sample {
                    timestamp_ms,
                    value,
                })
                .collect(),
            exemplars: s.exemplars.iter().map(exemplar).collect(),
        })
        .collect();
    MetricsResponse {
        series,
        metrics: RangeMetrics {
            completed_jobs: 1,
            total_jobs: 1,
        },
    }
}

/// Serializes the engine result to the compact Tempo-native JSON string
/// (also the byte-for-byte golden surface).
pub(crate) fn encode_json(result: &TraceMetricsResult) -> String {
    serde_json::to_string(&build(result)).expect("metrics response serializes")
}

/// The `application/json` HTTP response the metrics endpoints return.
pub(crate) fn encode_metrics(result: &TraceMetricsResult) -> Response {
    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        encode_json(result),
    )
        .into_response()
}

#[cfg(test)]
mod wire_literal {
    /// A reference-captured wire rendering (issue #237). Nothing textual
    /// or numeric leaves this module: every exit is a predicate returning
    /// `bool`/`usize`/index pairs, or the opaque `WireProbe`. There is no
    /// `value()`, no `tokens()`, and no raw-text accessor, conversion,
    /// deref or formatting impl of any spelling. `body.contains(lit)`
    /// therefore does not COMPILE, so the captured text has no accidental
    /// landing site outside this module. Deliberate reconstruction is possible and is a named,
    /// bounded residual, not a closed route: any predicate here is an
    /// equality oracle over caller-supplied candidates (residual R5 —
    /// external impls or free functions can reconstruct a rendering they
    /// already hold), and the guard scanner must `include_str!` this
    /// file, whose source necessarily contains every rendering in
    /// constructor position (residual R6). Both still need a search
    /// route outside Rule D's family to be useful. This whole block,
    /// including the attribute above it, is byte-frozen by the scanner
    /// (Rule C, upward-extended span); it invokes no macro, so no macro
    /// can be shadowed into it.
    pub(crate) struct WireLiteral(&'static str);

    /// Text handed to the single search path. Built only from a caller
    /// body or from `WireLiteral::surrounded_by`, and it has no reader.
    pub(crate) struct WireProbe(String);

    impl WireProbe {
        pub(crate) fn body(text: &str) -> Self {
            Self(String::from(text))
        }
    }

    impl WireLiteral {
        pub(crate) const fn new(text: &'static str) -> Self {
            Self(text)
        }

        /// True iff the captured text is EXACTLY what the locked encoder
        /// emits for `want`: it parses bit-identically to `want` AND
        /// `serde_json::to_string(&want)` reproduces it. This is the
        /// non-vacuity leg for every negative `occurs_in` assertion, and
        /// it pins all table copies to identical semantics per row.
        pub(crate) fn denotes(&self, want: f64) -> bool {
            let parses = match self.0.parse::<f64>() {
                Ok(v) => v.to_bits() == want.to_bits(),
                Err(_) => false,
            };
            let renders = match serde_json::to_string(&want) {
                Ok(s) => s == self.0,
                Err(_) => false,
            };
            parses && renders
        }

        /// The rendering as the `}`-closed JSON value token: a
        /// `Sample.value` is last in its object.
        fn closed(&self) -> String {
            let mut t = String::from("\"value\":");
            t.push_str(self.0);
            t.push('}');
            t
        }

        /// The rendering as the `,`-separated JSON value token: an
        /// `Exemplar.value` precedes `timestampMs`. A `Label.value`
        /// always opens an object, so it can never collide with a
        /// numeric token, and nothing but a quote can precede the
        /// `"value":` prefix, so there is no left-side hazard either.
        fn separated(&self) -> String {
            let mut t = String::from("\"value\":");
            t.push_str(self.0);
            t.push(',');
            t
        }

        /// The ONLY search: true iff the rendering appears as a DELIMITED
        /// JSON value token. Never bare — the two-rounding rendering of
        /// the 18_014_398_509_482_017 ns width is a prefix of the
        /// captured rendering of the 18_014_398_509_482_025 ns width,
        /// and the 1_128_000_000 ns captured rendering is a prefix of
        /// its own two-rounding form, so a bare check is wrong in BOTH
        /// directions.
        pub(crate) fn occurs_in(&self, probe: &WireProbe) -> bool {
            probe.0.contains(&self.closed()) || probe.0.contains(&self.separated())
        }

        /// This rendering wrapped in caller-chosen text, as a probe. The
        /// delimiter-sensitivity control runs through `occurs_in`, i.e.
        /// the same function the body assertions use.
        pub(crate) fn surrounded_by(&self, left: &str, right: &str) -> WireProbe {
            let mut t = String::from(left);
            t.push_str(self.0);
            t.push_str(right);
            WireProbe(t)
        }

        /// `(i, j)`, `i != j`, where cell `i`'s raw rendering is a
        /// substring of cell `j`'s — the BARE collision matrix, by index
        /// only.
        pub(crate) fn bare_collisions(cells: &[&Self]) -> Vec<(usize, usize)> {
            let mut out = Vec::new();
            for (i, a) in cells.iter().enumerate() {
                for (j, b) in cells.iter().enumerate() {
                    if i != j && b.0.contains(a.0) {
                        out.push((i, j));
                    }
                }
            }
            out
        }

        /// The same over the `2N` delimited tokens (cell `k` maps to
        /// tokens `2k` and `2k + 1`).
        pub(crate) fn token_collisions(cells: &[&Self]) -> Vec<(usize, usize)> {
            let mut tokens: Vec<String> = Vec::new();
            for cell in cells {
                tokens.push(cell.closed());
                tokens.push(cell.separated());
            }
            let mut out = Vec::new();
            for (i, a) in tokens.iter().enumerate() {
                for (j, b) in tokens.iter().enumerate() {
                    if i != j && b.contains(a) {
                        out.push((i, j));
                    }
                }
            }
            out
        }

        /// The rendering wrapped in double quotes — the source token Rule
        /// A counts.
        fn quoted(&self) -> String {
            let mut t = String::new();
            t.push('"');
            t.push_str(self.0);
            t.push('"');
            t
        }

        /// The same collision matrix over the `N` quote-wrapped SOURCE
        /// tokens — emptiness is what makes Rule A's counting
        /// unambiguous.
        pub(crate) fn quoted_collisions(cells: &[&Self]) -> Vec<(usize, usize)> {
            let mut out = Vec::new();
            for (i, a) in cells.iter().enumerate() {
                for (j, b) in cells.iter().enumerate() {
                    if i != j && b.quoted().contains(&a.quoted()) {
                        out.push((i, j));
                    }
                }
            }
            out
        }

        /// Occurrences of the quote-wrapped rendering in `src` (Rule A).
        pub(crate) fn count_quoted_in(&self, src: &str) -> usize {
            src.matches(&self.quoted()).count()
        }

        /// The rendering in constructor position, as source text.
        fn constructed(&self) -> String {
            let mut t = String::from("WireLiteral::new(");
            t.push_str(&self.quoted());
            t.push(')');
            t
        }

        /// Occurrences of the constructor-position rendering in `src`
        /// (Rule A).
        pub(crate) fn count_constructed_in(&self, src: &str) -> usize {
            src.matches(&self.constructed()).count()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulsus_read::{MetricLabel, MetricLabelValue, TraceMetricSeries};

    #[test]
    fn encoder_pins_the_captured_tempo_wire_shape_byte_for_byte() {
        // Two series: an ungrouped `rate` with a zero sample (value
        // omitted) and a positive one, plus a quantile series carrying a
        // `p` double label and a `trace:id` exemplar. Every captured
        // invariant (camelCase, timestampMs-as-string, value-omitted-at-
        // zero, AnyValue labels, trace:id exemplar) is exercised.
        let result = TraceMetricsResult {
            series: vec![
                TraceMetricSeries {
                    labels: vec![MetricLabel::str("__name__", "rate")],
                    samples: vec![
                        (1_784_796_060_000, 0.0),
                        (1_784_796_120_000, 0.8833333333333333),
                    ],
                    exemplars: vec![],
                },
                TraceMetricSeries {
                    labels: vec![
                        MetricLabel::str("resource.service.name", "checkout"),
                        MetricLabel {
                            key: "p".to_string(),
                            value: MetricLabelValue::Double(0.9),
                        },
                    ],
                    samples: vec![(1_784_796_120_000, 1.5)],
                    exemplars: vec![MetricExemplar {
                        labels: vec![MetricLabel::str("trace:id", "ceae79f2")],
                        value: 1.383,
                        timestamp_ms: 1_784_796_062_834,
                    }],
                },
            ],
        };
        let json = encode_json(&result);
        let expected = concat!(
            "{\"series\":[",
            "{\"labels\":[{\"key\":\"__name__\",\"value\":{\"stringValue\":\"rate\"}}],",
            "\"samples\":[{\"timestampMs\":\"1784796060000\"},",
            "{\"timestampMs\":\"1784796120000\",\"value\":0.8833333333333333}]},",
            "{\"labels\":[{\"key\":\"resource.service.name\",\"value\":{\"stringValue\":\"checkout\"}},",
            "{\"key\":\"p\",\"value\":{\"doubleValue\":0.9}}],",
            "\"samples\":[{\"timestampMs\":\"1784796120000\",\"value\":1.5}],",
            "\"exemplars\":[{\"labels\":[{\"key\":\"trace:id\",\"value\":{\"stringValue\":\"ceae79f2\"}}],",
            "\"value\":1.383,\"timestampMs\":\"1784796062834\"}]}",
            "],\"metrics\":{\"completedJobs\":1,\"totalJobs\":1}}",
        );
        assert_eq!(json, expected);
    }

    #[test]
    fn an_empty_result_still_carries_the_metrics_object() {
        let json = encode_json(&TraceMetricsResult { series: vec![] });
        assert_eq!(
            json,
            "{\"series\":[],\"metrics\":{\"completedJobs\":1,\"totalJobs\":1}}"
        );
    }

    // ------------------------------------------------------------------
    // Issue #237: the ns→seconds conversion pin, at the wire level, plus
    // the guard that keeps its assertions token-delimited and helper-only.
    // ------------------------------------------------------------------

    use super::wire_literal::{WireLiteral, WireProbe};

    /// Reference-captured ns→seconds renderings (issue #237).
    /// `grafana/tempo:3.0.2@sha256:cda87c21…`, probed 2026-07-26.
    /// `(ns, seconds value, captured rendering, two-rounding rendering)`.
    /// One copy per site by design — do NOT lift into a shared crate;
    /// Rule A cross-checks that this copy and `traces_api_live.rs`'s are
    /// transcribed identically.
    const REFERENCE_DURATION_SECONDS: &[(i64, f64, WireLiteral, WireLiteral)] = &[
        // ≤16-digit group: 1 ULP apart; pinned by the reference's own
        // comparison operator (`>= L` matches, `> L` does not).
        (
            1_118_000_000,
            1.118,
            WireLiteral::new("1.118"),
            WireLiteral::new("1.1179999999999999"),
        ),
        (
            1_122_000_000,
            1.122,
            WireLiteral::new("1.122"),
            WireLiteral::new("1.1219999999999999"),
        ),
        (
            1_128_000_000,
            1.128,
            WireLiteral::new("1.128"),
            WireLiteral::new("1.1280000000000001"),
        ),
        (
            1_235_000_000,
            1.235,
            WireLiteral::new("1.235"),
            WireLiteral::new("1.2349999999999999"),
        ),
        (
            31_952_000_000,
            31.952,
            WireLiteral::new("31.952"),
            WireLiteral::new("31.951999999999998"),
        ),
        (
            1_000_064_438,
            1.000064438,
            WireLiteral::new("1.000064438"),
            WireLiteral::new("1.0000644379999999"),
        ),
        // 17-significant-digit group: the formatter-independent RAW-WIRE
        // discriminators (#237 round 3). `ns > 2^53`, so the `int64->f64`
        // cast is lossy and the two-rounding value is the correctly
        // rounded one — the reference emitting the single-rounding value
        // positively identifies a cast-first form.
        (
            18_014_398_509_482_025,
            18_014_398.509_482_022,
            WireLiteral::new("18014398.509482022"),
            WireLiteral::new("18014398.509482026"),
        ),
        (
            18_014_398_509_482_035,
            18_014_398.509_482_037,
            WireLiteral::new("18014398.509482037"),
            WireLiteral::new("18014398.509482034"),
        ),
        (
            18_014_398_509_482_017,
            18_014_398.509_482_015,
            WireLiteral::new("18014398.509482015"),
            WireLiteral::new("18014398.50948202"),
        ),
        (
            1_088_608_058_291_172_412,
            1_088_608_058.291_172_3,
            WireLiteral::new("1088608058.2911723"),
            WireLiteral::new("1088608058.2911725"),
        ),
        (
            10_000_000_000_000_005,
            10_000_000.000_000_004,
            WireLiteral::new("10000000.000000004"),
            WireLiteral::new("10000000.000000006"),
        ),
        (
            10_000_000_000_000_015,
            10_000_000.000_000_017,
            WireLiteral::new("10000000.000000017"),
            WireLiteral::new("10000000.000000015"),
        ),
    ];

    /// Exactly representable under both rounding forms — these prove
    /// nothing on their own and exist only to catch a gross scaling
    /// error. Bit-level only: integral-double JSON rendering is a
    /// protojson number-format question (#263), not the ns→seconds
    /// conversion #237 settles, so controls carry NO wire literal and
    /// are never asserted as text.
    const REFERENCE_DURATION_CONTROLS: &[(i64, f64)] = &[
        (500_000_000, 0.5),
        (1_500_000_000, 1.5),
        (2_000_000_000, 2.0),
    ];

    /// The two-rounding form, transcribed ONLY so the tests can assert
    /// the production encoder input is NOT it (issues #237 / #232).
    fn two_rounding_seconds(ns: i64) -> f64 {
        (ns / 1_000_000_000) as f64 + (ns % 1_000_000_000) as f64 / 1e9
    }

    /// Issue #237 wire-level pin: the production encoder renders every
    /// captured reference value as the reference's own wire text, and
    /// never the two-rounding neighbour. The six 17-significant-digit
    /// entries are the load-bearing ones — no <=16-digit formatter can
    /// produce them. Hermetic; runs on every PR.
    #[test]
    fn duration_seconds_render_on_the_wire_exactly_as_the_reference_emits_them() {
        let mut series = Vec::new();
        for (i, (_, want, _, _)) in REFERENCE_DURATION_SECONDS.iter().enumerate() {
            series.push(TraceMetricSeries {
                labels: vec![MetricLabel::str("resource.service.name", format!("w{i}"))],
                samples: vec![(1_785_072_480_000, *want)],
                exemplars: vec![],
            });
        }
        for (i, (_, want)) in REFERENCE_DURATION_CONTROLS.iter().enumerate() {
            series.push(TraceMetricSeries {
                labels: vec![MetricLabel::str("resource.service.name", format!("c{i}"))],
                samples: vec![(1_785_072_480_000, *want)],
                exemplars: vec![],
            });
        }
        let body = encode_json(&TraceMetricsResult { series });
        let probe = WireProbe::body(&body);
        for (ns, want, s, t) in REFERENCE_DURATION_SECONDS {
            assert!(s.denotes(*want), "{ns}: transcription");
            assert!(s.occurs_in(&probe), "{ns}: captured rendering");
            assert!(!t.occurs_in(&probe), "{ns}: two-rounding");
        }
        // Controls: decoded bit-equality only — their wire text is #263's
        // integral-double question, deliberately not asserted here.
        let decoded: serde_json::Value = serde_json::from_str(&body).expect("body parses");
        for (i, (ns, want)) in REFERENCE_DURATION_CONTROLS.iter().enumerate() {
            let got = decoded["series"][12 + i]["samples"][0]["value"]
                .as_f64()
                .expect("control sample value");
            assert_eq!(got.to_bits(), want.to_bits(), "{ns}: control bits");
        }
    }

    /// Issue #237 guard, behavioural half: legs (a)–(f) of plan v7/v8.
    #[test]
    fn wire_value_assertions_must_be_token_delimited() {
        // (a) cardinality, then derivation/non-vacuity for all 24 cells:
        // each rendering is exactly what the locked encoder emits for its
        // f64 (the T side pinned to `two_rounding_seconds`, computed —
        // never a re-typed constant).
        assert_eq!(REFERENCE_DURATION_SECONDS.len(), EXPECTED_WIRE_ROWS);
        let mut cells: Vec<&WireLiteral> = Vec::new();
        let mut keys: Vec<(i64, char)> = Vec::new();
        let mut wants: Vec<f64> = Vec::new();
        for (ns, want, s, t) in REFERENCE_DURATION_SECONDS {
            cells.push(s);
            keys.push((*ns, 'S'));
            wants.push(*want);
            cells.push(t);
            keys.push((*ns, 'T'));
            wants.push(two_rounding_seconds(*ns));
        }
        assert_eq!(cells.len(), 24);
        for (i, lit) in cells.iter().enumerate() {
            assert!(lit.denotes(wants[i]), "cell {:?}", keys[i]);
        }
        // (b) the bare collision matrix equals the frozen 3-relation set,
        // keyed by (ns, side) — a fourth OR a vanished relation fails.
        let got: Vec<((i64, char), (i64, char))> = WireLiteral::bare_collisions(&cells)
            .into_iter()
            .map(|(i, j)| (keys[i], keys[j]))
            .collect();
        assert_eq!(
            got,
            vec![
                ((1_128_000_000, 'S'), (1_128_000_000, 'T')),
                ((18_014_398_509_482_017, 'T'), (18_014_398_509_482_025, 'S')),
                ((18_014_398_509_482_017, 'T'), (18_014_398_509_482_025, 'T')),
            ],
            "bare-substring relations over the 24 renderings"
        );
        // (c) the delimited 48-token matrix is empty.
        assert_eq!(WireLiteral::token_collisions(&cells), vec![]);
        // (d) the quote-wrapped source-token matrix is empty — Rule A's
        // occurrence counting is unambiguous.
        assert_eq!(WireLiteral::quoted_collisions(&cells), vec![]);
        // (e) delimiter sensitivity, through the SAME `occurs_in` the
        // body assertions use.
        for lit in &cells {
            assert!(!lit.occurs_in(&lit.surrounded_by("prefix", "suffix")));
            assert!(lit.occurs_in(&lit.surrounded_by("{\"value\":", "}")));
            let inside = lit.surrounded_by("{\"value\":", ",\"timestampMs\":\"1\"}");
            assert!(lit.occurs_in(&inside));
        }
        // (f) the module's own predicates must discriminate: no leg above
        // can pass vacuously.
        for (i, lit) in cells.iter().enumerate() {
            let neighbour = f64::from_bits(wants[i].to_bits() ^ 1);
            assert!(!lit.denotes(neighbour), "cell {:?}", keys[i]);
        }
        let a = WireLiteral::new("7.25");
        let b = WireLiteral::new("7.25");
        let pair: Vec<&WireLiteral> = vec![&a, &b];
        assert!(!WireLiteral::bare_collisions(&pair).is_empty());
        assert!(!WireLiteral::token_collisions(&pair).is_empty());
        assert!(!WireLiteral::quoted_collisions(&pair).is_empty());
        let seven = format!("{}", 7.25_f64);
        let mut src_with = String::from("let a = WireLiteral::new(");
        src_with.push('"');
        src_with.push_str(&seven);
        src_with.push('"');
        src_with.push(')');
        src_with.push(';');
        assert_eq!(a.count_quoted_in(&src_with), 1);
        assert_eq!(a.count_constructed_in(&src_with), 1);
        assert_eq!(a.count_quoted_in("no rendering here"), 0);
        assert_eq!(a.count_constructed_in("no rendering here"), 0);
    }

    // ------------------------------------------------------------------
    // The scanner (issue #237 Rules A, B, C-freeze, C'-exempt-pin, D, E).
    // Violations are returned rather than asserted so the scanner itself
    // is testable on planted input. Emission order: lexer -> A -> B -> C
    // -> C' -> D -> E; within Rule A in table order (row, S before T).
    // The claim is bounded: it blocks every accidental route; deliberate
    // reconstruction (residuals R5/R6 in `mod wire_literal`'s doc) is
    // named, not closed, and no further rule should be added to chase it.
    // ------------------------------------------------------------------

    /// Frozen per-file scan configuration. Rule A's cell set is NOT a
    /// parameter: it is `REFERENCE_DURATION_SECONDS`, the table this
    /// scanner lives beside, so no caller can hand the scanner a short or
    /// empty set.
    struct WireScanSpec {
        file: &'static str,
        /// Rule C: the byte-frozen `mod wire_literal` block, line for
        /// line, upward-extended to the previous column-0 `}` so attached
        /// attributes are inside the frozen text.
        frozen_module: &'static [&'static str],
        /// Rule C' / D-exemption: `(fn name, byte-frozen lines including
        /// signature and closer)`. The exemption cannot be repurposed —
        /// emptying the list un-exempts the helper and makes Rule D fire
        /// on its body instead of disabling anything.
        frozen_search_helpers: &'static [(&'static str, &'static [&'static str])],
        /// Rule E: test spans needing one positive and one negative
        /// `occurs_in` assertion each.
        wire_tests: &'static [&'static str],
    }

    const EXPECTED_WIRE_ROWS: usize = 12;

    const WHAT_TEST_FN: &str = "wire test fn";
    const WHAT_POSITIVE: &str = "positive occurs_in assertion";
    const WHAT_NEGATIVE: &str = "negative occurs_in assertion";

    /// A machine-checkable breach of the wire-assertion discipline.
    #[derive(Debug, PartialEq, Eq)]
    enum WireScanViolation {
        BlockCommentUnsupported {
            file: &'static str,
            line: usize,
        },
        /// Rule A — identified by table cell index, never by text.
        LooseCapturedLiteral {
            file: &'static str,
            index: usize,
            quoted: usize,
            constructed: usize,
        },
        /// Rule B.
        BareDecimalLiteral {
            file: &'static str,
            line: usize,
            literal: String,
        },
        /// Rule C.
        MissingWireLiteralModule {
            file: &'static str,
        },
        DuplicateWireLiteralModule {
            file: &'static str,
        },
        WireLiteralModuleNotFrozen {
            file: &'static str,
            line: usize,
            found: String,
            want: String,
        },
        /// Rule D.
        UnguardedSearchCall {
            file: &'static str,
            line: usize,
            text: String,
        },
        /// Rule C' (the D-exemption pin).
        ExemptHelperBodyChanged {
            file: &'static str,
            helper: &'static str,
            line: usize,
        },
        /// Rule E.
        MissingWireAssertion {
            file: &'static str,
            test: &'static str,
            what: &'static str,
        },
        /// Rule A cardinality: the table backing the scan is not 12 rows.
        /// Emitted alone (the scan short-circuits), so a shrunken table
        /// is a failure, never a quiet pass.
        TableCardinalityMismatch {
            file: &'static str,
            rows: usize,
            want: usize,
        },
        /// Rule E cardinality: `spec.wire_tests` is empty.
        EmptyWireTestList {
            file: &'static str,
        },
    }

    /// Rule D's frozen substring-search family (#237 plan v8): `str`
    /// pattern APIs plus slice `windows`, in method position. The
    /// split-family, iterator `any`/`position`, `char_indices`,
    /// `replace`/`replacen`, regex/memchr and hand-rolled indexed scans
    /// are deliberately OUTSIDE it (residual R3) — do not widen it; the
    /// widening was measured to flag verified-legitimate call sites.
    const RULE_D_FAMILY: &[&str] = &[
        "contains",
        "starts_with",
        "ends_with",
        "find",
        "rfind",
        "matches",
        "rmatches",
        "match_indices",
        "rmatch_indices",
        "strip_prefix",
        "strip_suffix",
        "trim_matches",
        "trim_start_matches",
        "trim_end_matches",
        "windows",
    ];

    /// Hand-rolled substring search. The scanner scans its own source
    /// under Rule D, so it must never itself call a pattern API with a
    /// derived needle; an indexed scan is the sanctioned spelling here.
    fn find_sub(hay: &str, needle: &str, from: usize) -> Option<usize> {
        let h = hay.as_bytes();
        let n = needle.as_bytes();
        if n.is_empty() || h.len() < n.len() {
            return None;
        }
        let mut i = from;
        while i + n.len() <= h.len() {
            if &h[i..i + n.len()] == n {
                return Some(i);
            }
            i += 1;
        }
        None
    }

    /// `^[0-9]+\.[0-9]+([eE][+-]?[0-9]+)?$` without a regex dependency.
    fn is_bare_decimal(s: &str) -> bool {
        let b = s.as_bytes();
        let mut i = 0;
        let mut int_digits = 0;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
            int_digits += 1;
        }
        if int_digits == 0 || i >= b.len() || b[i] != b'.' {
            return false;
        }
        i += 1;
        let mut frac_digits = 0;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
            frac_digits += 1;
        }
        if frac_digits == 0 {
            return false;
        }
        if i == b.len() {
            return true;
        }
        if b[i] != b'e' && b[i] != b'E' {
            return false;
        }
        i += 1;
        if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
            i += 1;
        }
        let mut exp_digits = 0;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
            exp_digits += 1;
        }
        exp_digits > 0 && i == b.len()
    }

    /// One extracted string/byte-string literal (delimiters excluded,
    /// escapes NOT decoded). `col` is the byte offset of the opening
    /// quote on its (masked) start line; `prefix` the masked code before
    /// it.
    struct SrcLiteral {
        line: usize,
        col: usize,
        content: String,
        prefix: String,
        byte_string: bool,
    }

    struct LexedSource {
        /// Per-line masked code: literal contents and comment text
        /// replaced by `\u{1}` char-for-char (delimiters kept).
        masked: Vec<String>,
        literals: Vec<SrcLiteral>,
        violations: Vec<WireScanViolation>,
    }

    /// The single escape-aware pass: strips `//` comments (outside
    /// literals), extracts string/byte-string/raw-string literals,
    /// handles char literals vs lifetimes, and rejects block comments
    /// outright so it can never be silently wrong.
    fn lex_source(file: &'static str, src: &str) -> LexedSource {
        struct StrState {
            byte_string: bool,
            raw_hashes: Option<usize>,
            line: usize,
            col: usize,
            prefix: String,
            buf: String,
        }
        let mut masked: Vec<String> = Vec::new();
        let mut literals: Vec<SrcLiteral> = Vec::new();
        let mut violations: Vec<WireScanViolation> = Vec::new();
        let mut in_str: Option<StrState> = None;

        for (li, line) in src.lines().enumerate() {
            let n = li + 1;
            let chars: Vec<char> = line.chars().collect();
            let mut out = String::new();
            let mut i = 0usize;
            while i < chars.len() {
                if let Some(st) = in_str.as_mut() {
                    let c = chars[i];
                    if let Some(hashes) = st.raw_hashes {
                        let mut ends = c == '"';
                        if ends {
                            for k in 0..hashes {
                                if chars.get(i + 1 + k) != Some(&'#') {
                                    ends = false;
                                }
                            }
                        }
                        if ends {
                            let st = in_str.take().expect("in raw string");
                            literals.push(SrcLiteral {
                                line: st.line,
                                col: st.col,
                                content: st.buf,
                                prefix: st.prefix,
                                byte_string: st.byte_string,
                            });
                            out.push('"');
                            for _ in 0..hashes {
                                out.push('#');
                            }
                            i += 1 + hashes;
                        } else {
                            st.buf.push(c);
                            out.push('\u{1}');
                            i += 1;
                        }
                    } else if c == '\\' {
                        st.buf.push('\\');
                        out.push('\u{1}');
                        i += 1;
                        if i < chars.len() {
                            st.buf.push(chars[i]);
                            out.push('\u{1}');
                            i += 1;
                        }
                    } else if c == '"' {
                        let st = in_str.take().expect("in string");
                        literals.push(SrcLiteral {
                            line: st.line,
                            col: st.col,
                            content: st.buf,
                            prefix: st.prefix,
                            byte_string: st.byte_string,
                        });
                        out.push('"');
                        i += 1;
                    } else {
                        st.buf.push(c);
                        out.push('\u{1}');
                        i += 1;
                    }
                    continue;
                }
                let c = chars[i];
                if c == '/' && chars.get(i + 1) == Some(&'/') {
                    out.push('/');
                    out.push('/');
                    i += 2;
                    while i < chars.len() {
                        out.push('\u{1}');
                        i += 1;
                    }
                } else if c == '/' && chars.get(i + 1) == Some(&'*') {
                    violations.push(WireScanViolation::BlockCommentUnsupported { file, line: n });
                    out.push(c);
                    i += 1;
                } else if c == '"' {
                    let mut prev_chars = out.chars().rev();
                    let prev = prev_chars.next();
                    let prev2 = prev_chars.next();
                    let byte_string = prev == Some('b')
                        && !prev2.is_some_and(|p| p.is_ascii_alphanumeric() || p == '_');
                    let col = out.len();
                    let prefix = out.clone();
                    out.push('"');
                    i += 1;
                    in_str = Some(StrState {
                        byte_string,
                        raw_hashes: None,
                        line: n,
                        col,
                        prefix,
                        buf: String::new(),
                    });
                } else if c == 'r' && {
                    let mut j = i + 1;
                    while chars.get(j) == Some(&'#') {
                        j += 1;
                    }
                    chars.get(j) == Some(&'"')
                } {
                    let mut j = i + 1;
                    let mut hashes = 0usize;
                    while chars.get(j) == Some(&'#') {
                        hashes += 1;
                        j += 1;
                    }
                    let prefix = out.clone();
                    out.push('r');
                    for _ in 0..hashes {
                        out.push('#');
                    }
                    let col = out.len();
                    out.push('"');
                    i = j + 1;
                    in_str = Some(StrState {
                        byte_string: false,
                        raw_hashes: Some(hashes),
                        line: n,
                        col,
                        prefix,
                        buf: String::new(),
                    });
                } else if c == '\'' {
                    if chars.get(i + 1) == Some(&'\\') {
                        out.push('\'');
                        out.push('\u{1}');
                        out.push('\u{1}');
                        i += 3;
                        if chars.get(i) == Some(&'\'') {
                            out.push('\'');
                            i += 1;
                        }
                    } else if chars.get(i + 2) == Some(&'\'') && i + 1 < chars.len() {
                        out.push('\'');
                        out.push('\u{1}');
                        out.push('\'');
                        i += 3;
                    } else {
                        out.push('\'');
                        i += 1;
                    }
                } else {
                    out.push(c);
                    i += 1;
                }
            }
            if let Some(st) = in_str.as_mut() {
                st.buf.push('\n');
            }
            masked.push(out);
        }
        LexedSource {
            masked,
            literals,
            violations,
        }
    }

    /// The string-literal argument starting at masked byte `col` on
    /// 1-based `line`, if the lexer recorded one there.
    fn literal_at(literals: &[SrcLiteral], line: usize, col: usize) -> Option<&SrcLiteral> {
        literals.iter().find(|l| l.line == line && l.col == col)
    }

    /// Rule D's argument carve-out: a plain string/byte-string literal
    /// that is not a bare decimal, a char literal, or a closure.
    fn arg_is_guarded(literals: &[SrcLiteral], line: usize, mline: &str, arg_start: usize) -> bool {
        let b = mline.as_bytes();
        let mut p = arg_start;
        while p < b.len() && b[p] == b' ' {
            p += 1;
        }
        if p >= b.len() {
            // The call wraps to the next line — conservative, loud.
            return false;
        }
        match b[p] {
            b'"' => literal_at(literals, line, p).is_some_and(|l| !is_bare_decimal(&l.content)),
            b'b' if p + 1 < b.len() && b[p + 1] == b'"' => {
                literal_at(literals, line, p + 1).is_some_and(|l| !is_bare_decimal(&l.content))
            }
            b'\'' => true,
            b'|' => true,
            b'm' => mline[p..].starts_with("move |"),
            _ => false,
        }
    }

    /// Skips the first argument of a two-argument call (needle-last
    /// crate-local helpers like `find_subslice(hay, needle)`), returning
    /// the byte offset just past the top-level comma.
    fn second_arg_start(mline: &str, arg_start: usize) -> Option<usize> {
        let b = mline.as_bytes();
        let mut depth = 0i32;
        let mut p = arg_start;
        while p < b.len() {
            match b[p] {
                b'(' | b'[' | b'{' => depth += 1,
                b')' | b']' | b'}' => {
                    if depth == 0 {
                        return None;
                    }
                    depth -= 1;
                }
                b',' if depth == 0 => return Some(p + 1),
                _ => {}
            }
            p += 1;
        }
        None
    }

    fn is_fn_decl(trimmed: &str) -> bool {
        trimmed.starts_with("fn ")
            || trimmed.starts_with("pub fn ")
            || trimmed.starts_with("pub(crate) fn ")
            || trimmed.starts_with("pub(super) fn ")
            || trimmed.starts_with("async fn ")
    }

    /// The public scan: Rule A's cell set is the module-local table, not
    /// a parameter — do NOT re-add a `captured` parameter in any form.
    fn scan_wire_assertions(spec: &WireScanSpec, src: &str) -> Vec<WireScanViolation> {
        scan_with_table(spec, src, REFERENCE_DURATION_SECONDS)
    }

    /// The table-parameterised core, reachable only from
    /// `scan_wire_assertions` (which binds the real table) and from the
    /// planted-violation self-test (which must be able to plant a
    /// `TableCardinalityMismatch`).
    fn scan_with_table(
        spec: &WireScanSpec,
        src: &str,
        table: &[(i64, f64, WireLiteral, WireLiteral)],
    ) -> Vec<WireScanViolation> {
        let file = spec.file;
        if table.len() != EXPECTED_WIRE_ROWS {
            return vec![WireScanViolation::TableCardinalityMismatch {
                file,
                rows: table.len(),
                want: EXPECTED_WIRE_ROWS,
            }];
        }
        let lex = lex_source(file, src);
        let mut out = lex.violations;
        let raw_lines: Vec<&str> = src.lines().collect();

        // Rule A — each captured rendering occurs exactly once quoted and
        // once in constructor position, per file.
        for (row, (_, _, s_lit, t_lit)) in table.iter().enumerate() {
            for (side, lit) in [(0usize, s_lit), (1usize, t_lit)] {
                let quoted = lit.count_quoted_in(src);
                let constructed = lit.count_constructed_in(src);
                if quoted != 1 || constructed != 1 {
                    out.push(WireScanViolation::LooseCapturedLiteral {
                        file,
                        index: row * 2 + side,
                        quoted,
                        constructed,
                    });
                }
            }
        }

        // Rule B — no whole-content bare-decimal string literal outside
        // `WireLiteral::new(…)` argument position.
        for l in &lex.literals {
            if !l.byte_string
                && is_bare_decimal(&l.content)
                && !l.prefix.trim_end().ends_with("WireLiteral::new(")
            {
                out.push(WireScanViolation::BareDecimalLiteral {
                    file,
                    line: l.line,
                    literal: l.content.clone(),
                });
            }
        }

        // Rule C — the byte-frozen, upward-extended module span.
        let mut exempt: Vec<(usize, usize)> = Vec::new();
        let mut mod_lines: Vec<usize> = Vec::new();
        for (i, mline) in lex.masked.iter().enumerate() {
            if mline.trim() == "mod wire_literal {" {
                mod_lines.push(i);
            }
        }
        if mod_lines.is_empty() {
            out.push(WireScanViolation::MissingWireLiteralModule { file });
        } else if mod_lines.len() > 1 {
            out.push(WireScanViolation::DuplicateWireLiteralModule { file });
        } else {
            let m = mod_lines[0];
            let mut e = raw_lines.len().saturating_sub(1);
            for (i, mline) in lex.masked.iter().enumerate().skip(m + 1) {
                if mline == "}" {
                    e = i;
                    break;
                }
            }
            let mut s = 0usize;
            for i in (0..m).rev() {
                if lex.masked[i] == "}" {
                    s = i + 1;
                    break;
                }
            }
            while s < m && raw_lines[s].trim().is_empty() {
                s += 1;
            }
            let span_len = e - s + 1;
            let longest = span_len.max(spec.frozen_module.len());
            for k in 0..longest {
                let found = if k < span_len { raw_lines[s + k] } else { "" };
                let want = spec.frozen_module.get(k).copied().unwrap_or("");
                if found != want {
                    out.push(WireScanViolation::WireLiteralModuleNotFrozen {
                        file,
                        line: s + k + 1,
                        found: String::from(found),
                        want: String::from(want),
                    });
                    break;
                }
            }
            exempt.push((s, e));
        }

        // Rule C' — the D-exempt helper bodies are byte-frozen, so the
        // exemption cannot be repurposed into a bare scanner.
        for (helper, frozen_lines) in spec.frozen_search_helpers {
            let first = frozen_lines.first().copied().unwrap_or("");
            let mut at = None;
            if !first.is_empty() {
                for (i, l) in raw_lines.iter().enumerate() {
                    if *l == first {
                        at = Some(i);
                        break;
                    }
                }
            }
            match at {
                None => out.push(WireScanViolation::ExemptHelperBodyChanged {
                    file,
                    helper,
                    line: 0,
                }),
                Some(k) => {
                    let mut ok = true;
                    for (off, want) in frozen_lines.iter().enumerate() {
                        let found = raw_lines.get(k + off).copied().unwrap_or("");
                        if found != *want {
                            out.push(WireScanViolation::ExemptHelperBodyChanged {
                                file,
                                helper,
                                line: k + off + 1,
                            });
                            ok = false;
                            break;
                        }
                    }
                    if ok {
                        exempt.push((k, k + frozen_lines.len() - 1));
                    }
                }
            }
        }

        // Rule D — family search calls need a literal (non-decimal),
        // char-literal or closure needle, outside the exempt spans.
        for (i, mline) in lex.masked.iter().enumerate() {
            if exempt.iter().any(|(a, b)| i >= *a && i <= *b) {
                continue;
            }
            if is_fn_decl(mline.trim_start()) {
                continue;
            }
            let mut flagged = false;
            for name in RULE_D_FAMILY {
                let mut needle = String::from(".");
                needle.push_str(name);
                needle.push('(');
                let mut from = 0usize;
                while let Some(p) = find_sub(mline, &needle, from) {
                    from = p + 1;
                    if !arg_is_guarded(&lex.literals, i + 1, mline, p + needle.len()) {
                        flagged = true;
                    }
                }
            }
            for (helper, _) in spec.frozen_search_helpers {
                let mut needle = String::from(*helper);
                needle.push('(');
                let mut from = 0usize;
                while let Some(p) = find_sub(mline, &needle, from) {
                    from = p + 1;
                    if p > 0 {
                        let prev = mline.as_bytes()[p - 1];
                        if prev == b'.' || prev == b'_' || prev.is_ascii_alphanumeric() || prev == 1
                        {
                            continue;
                        }
                    }
                    match second_arg_start(mline, p + needle.len()) {
                        Some(a) => {
                            if !arg_is_guarded(&lex.literals, i + 1, mline, a) {
                                flagged = true;
                            }
                        }
                        None => flagged = true,
                    }
                }
            }
            if flagged {
                out.push(WireScanViolation::UnguardedSearchCall {
                    file,
                    line: i + 1,
                    text: String::from(raw_lines[i].trim()),
                });
            }
        }

        // Rule E — every named wire test carries one positive and one
        // negative `occurs_in` assertion.
        if spec.wire_tests.is_empty() {
            out.push(WireScanViolation::EmptyWireTestList { file });
        }
        for test in spec.wire_tests {
            let mut needle = String::from("fn ");
            needle.push_str(test);
            needle.push('(');
            let mut decl = None;
            for (i, mline) in lex.masked.iter().enumerate() {
                if find_sub(mline, &needle, 0).is_some() {
                    decl = Some(i);
                    break;
                }
            }
            let Some(d) = decl else {
                out.push(WireScanViolation::MissingWireAssertion {
                    file,
                    test,
                    what: WHAT_TEST_FN,
                });
                continue;
            };
            let decl_line = raw_lines[d];
            let indent = decl_line.len() - decl_line.trim_start().len();
            let mut closer = String::new();
            for _ in 0..indent {
                closer.push(' ');
            }
            closer.push('}');
            let mut end = lex.masked.len();
            for (i, mline) in lex.masked.iter().enumerate().skip(d + 1) {
                if *mline == closer {
                    end = i;
                    break;
                }
            }
            let mut has_pos = false;
            let mut has_neg = false;
            for mline in &lex.masked[d + 1..end] {
                if !mline.contains(".occurs_in(") {
                    continue;
                }
                if mline.contains("assert!(!") {
                    has_neg = true;
                } else if mline.contains("assert!(") {
                    has_pos = true;
                }
            }
            if !has_pos {
                out.push(WireScanViolation::MissingWireAssertion {
                    file,
                    test,
                    what: WHAT_POSITIVE,
                });
            }
            if !has_neg {
                out.push(WireScanViolation::MissingWireAssertion {
                    file,
                    test,
                    what: WHAT_NEGATIVE,
                });
            }
        }
        out
    }

    /// The byte-frozen text of this file's `mod wire_literal` block,
    /// including the `#[cfg(test)]` line above it (Rule C's
    /// upward-extended span). Regenerated only as a deliberate, reviewed
    /// edit alongside the module itself.
    const FROZEN_WIRE_LITERAL_METRICS_RESPONSE: &[&str] = &[
        "#[cfg(test)]",
        "mod wire_literal {",
        "    /// A reference-captured wire rendering (issue #237). Nothing textual",
        "    /// or numeric leaves this module: every exit is a predicate returning",
        "    /// `bool`/`usize`/index pairs, or the opaque `WireProbe`. There is no",
        "    /// `value()`, no `tokens()`, and no raw-text accessor, conversion,",
        "    /// deref or formatting impl of any spelling. `body.contains(lit)`",
        "    /// therefore does not COMPILE, so the captured text has no accidental",
        "    /// landing site outside this module. Deliberate reconstruction is possible and is a named,",
        "    /// bounded residual, not a closed route: any predicate here is an",
        "    /// equality oracle over caller-supplied candidates (residual R5 —",
        "    /// external impls or free functions can reconstruct a rendering they",
        "    /// already hold), and the guard scanner must `include_str!` this",
        "    /// file, whose source necessarily contains every rendering in",
        "    /// constructor position (residual R6). Both still need a search",
        "    /// route outside Rule D's family to be useful. This whole block,",
        "    /// including the attribute above it, is byte-frozen by the scanner",
        "    /// (Rule C, upward-extended span); it invokes no macro, so no macro",
        "    /// can be shadowed into it.",
        "    pub(crate) struct WireLiteral(&'static str);",
        "",
        "    /// Text handed to the single search path. Built only from a caller",
        "    /// body or from `WireLiteral::surrounded_by`, and it has no reader.",
        "    pub(crate) struct WireProbe(String);",
        "",
        "    impl WireProbe {",
        "        pub(crate) fn body(text: &str) -> Self {",
        "            Self(String::from(text))",
        "        }",
        "    }",
        "",
        "    impl WireLiteral {",
        "        pub(crate) const fn new(text: &'static str) -> Self {",
        "            Self(text)",
        "        }",
        "",
        "        /// True iff the captured text is EXACTLY what the locked encoder",
        "        /// emits for `want`: it parses bit-identically to `want` AND",
        "        /// `serde_json::to_string(&want)` reproduces it. This is the",
        "        /// non-vacuity leg for every negative `occurs_in` assertion, and",
        "        /// it pins all table copies to identical semantics per row.",
        "        pub(crate) fn denotes(&self, want: f64) -> bool {",
        "            let parses = match self.0.parse::<f64>() {",
        "                Ok(v) => v.to_bits() == want.to_bits(),",
        "                Err(_) => false,",
        "            };",
        "            let renders = match serde_json::to_string(&want) {",
        "                Ok(s) => s == self.0,",
        "                Err(_) => false,",
        "            };",
        "            parses && renders",
        "        }",
        "",
        "        /// The rendering as the `}`-closed JSON value token: a",
        "        /// `Sample.value` is last in its object.",
        "        fn closed(&self) -> String {",
        "            let mut t = String::from(\"\\\"value\\\":\");",
        "            t.push_str(self.0);",
        "            t.push('}');",
        "            t",
        "        }",
        "",
        "        /// The rendering as the `,`-separated JSON value token: an",
        "        /// `Exemplar.value` precedes `timestampMs`. A `Label.value`",
        "        /// always opens an object, so it can never collide with a",
        "        /// numeric token, and nothing but a quote can precede the",
        "        /// `\"value\":` prefix, so there is no left-side hazard either.",
        "        fn separated(&self) -> String {",
        "            let mut t = String::from(\"\\\"value\\\":\");",
        "            t.push_str(self.0);",
        "            t.push(',');",
        "            t",
        "        }",
        "",
        "        /// The ONLY search: true iff the rendering appears as a DELIMITED",
        "        /// JSON value token. Never bare — the two-rounding rendering of",
        "        /// the 18_014_398_509_482_017 ns width is a prefix of the",
        "        /// captured rendering of the 18_014_398_509_482_025 ns width,",
        "        /// and the 1_128_000_000 ns captured rendering is a prefix of",
        "        /// its own two-rounding form, so a bare check is wrong in BOTH",
        "        /// directions.",
        "        pub(crate) fn occurs_in(&self, probe: &WireProbe) -> bool {",
        "            probe.0.contains(&self.closed()) || probe.0.contains(&self.separated())",
        "        }",
        "",
        "        /// This rendering wrapped in caller-chosen text, as a probe. The",
        "        /// delimiter-sensitivity control runs through `occurs_in`, i.e.",
        "        /// the same function the body assertions use.",
        "        pub(crate) fn surrounded_by(&self, left: &str, right: &str) -> WireProbe {",
        "            let mut t = String::from(left);",
        "            t.push_str(self.0);",
        "            t.push_str(right);",
        "            WireProbe(t)",
        "        }",
        "",
        "        /// `(i, j)`, `i != j`, where cell `i`'s raw rendering is a",
        "        /// substring of cell `j`'s — the BARE collision matrix, by index",
        "        /// only.",
        "        pub(crate) fn bare_collisions(cells: &[&Self]) -> Vec<(usize, usize)> {",
        "            let mut out = Vec::new();",
        "            for (i, a) in cells.iter().enumerate() {",
        "                for (j, b) in cells.iter().enumerate() {",
        "                    if i != j && b.0.contains(a.0) {",
        "                        out.push((i, j));",
        "                    }",
        "                }",
        "            }",
        "            out",
        "        }",
        "",
        "        /// The same over the `2N` delimited tokens (cell `k` maps to",
        "        /// tokens `2k` and `2k + 1`).",
        "        pub(crate) fn token_collisions(cells: &[&Self]) -> Vec<(usize, usize)> {",
        "            let mut tokens: Vec<String> = Vec::new();",
        "            for cell in cells {",
        "                tokens.push(cell.closed());",
        "                tokens.push(cell.separated());",
        "            }",
        "            let mut out = Vec::new();",
        "            for (i, a) in tokens.iter().enumerate() {",
        "                for (j, b) in tokens.iter().enumerate() {",
        "                    if i != j && b.contains(a) {",
        "                        out.push((i, j));",
        "                    }",
        "                }",
        "            }",
        "            out",
        "        }",
        "",
        "        /// The rendering wrapped in double quotes — the source token Rule",
        "        /// A counts.",
        "        fn quoted(&self) -> String {",
        "            let mut t = String::new();",
        "            t.push('\"');",
        "            t.push_str(self.0);",
        "            t.push('\"');",
        "            t",
        "        }",
        "",
        "        /// The same collision matrix over the `N` quote-wrapped SOURCE",
        "        /// tokens — emptiness is what makes Rule A's counting",
        "        /// unambiguous.",
        "        pub(crate) fn quoted_collisions(cells: &[&Self]) -> Vec<(usize, usize)> {",
        "            let mut out = Vec::new();",
        "            for (i, a) in cells.iter().enumerate() {",
        "                for (j, b) in cells.iter().enumerate() {",
        "                    if i != j && b.quoted().contains(&a.quoted()) {",
        "                        out.push((i, j));",
        "                    }",
        "                }",
        "            }",
        "            out",
        "        }",
        "",
        "        /// Occurrences of the quote-wrapped rendering in `src` (Rule A).",
        "        pub(crate) fn count_quoted_in(&self, src: &str) -> usize {",
        "            src.matches(&self.quoted()).count()",
        "        }",
        "",
        "        /// The rendering in constructor position, as source text.",
        "        fn constructed(&self) -> String {",
        "            let mut t = String::from(\"WireLiteral::new(\");",
        "            t.push_str(&self.quoted());",
        "            t.push(')');",
        "            t",
        "        }",
        "",
        "        /// Occurrences of the constructor-position rendering in `src`",
        "        /// (Rule A).",
        "        pub(crate) fn count_constructed_in(&self, src: &str) -> usize {",
        "            src.matches(&self.constructed()).count()",
        "        }",
        "    }",
        "}",
    ];

    /// The byte-frozen text of `traces_api_live.rs`' `mod wire_literal`
    /// block (bytes flavour; no attribute above it — an integration-test
    /// file is all test code).
    const FROZEN_WIRE_LITERAL_TRACES_API_LIVE: &[&str] = &[
        "mod wire_literal {",
        "    /// A reference-captured wire rendering (issue #237), bytes flavour.",
        "    /// Nothing textual or numeric leaves this module: every exit is a",
        "    /// predicate or the opaque `WireProbe`. There is no `value()`, no",
        "    /// `tokens()`, and no raw-text accessor, conversion, deref or",
        "    /// formatting impl of any spelling, so a bare",
        "    /// `find_subslice(&body, lit)` does not COMPILE — the captured text",
        "    /// has no accidental landing site. This whole",
        "    /// block is byte-frozen by the `metrics_response.rs` scanner (Rule",
        "    /// C, upward-extended span); it invokes no macro and does not depend",
        "    /// on `find_subslice` (its search is its own, inside the frozen",
        "    /// span).",
        "    pub(crate) struct WireLiteral(&'static str);",
        "",
        "    /// Raw HTTP body bytes handed to the single search path. Built only",
        "    /// from a caller body or `WireLiteral::surrounded_by`; no reader.",
        "    pub(crate) struct WireProbe(Vec<u8>);",
        "",
        "    impl WireProbe {",
        "        pub(crate) fn body(bytes: &[u8]) -> Self {",
        "            Self(bytes.to_vec())",
        "        }",
        "    }",
        "",
        "    impl WireLiteral {",
        "        pub(crate) const fn new(text: &'static str) -> Self {",
        "            Self(text)",
        "        }",
        "",
        "        /// True iff the captured text is EXACTLY what the locked encoder",
        "        /// emits for `want`: it parses bit-identically to `want` AND",
        "        /// `serde_json::to_string(&want)` reproduces it.",
        "        pub(crate) fn denotes(&self, want: f64) -> bool {",
        "            let parses = match self.0.parse::<f64>() {",
        "                Ok(v) => v.to_bits() == want.to_bits(),",
        "                Err(_) => false,",
        "            };",
        "            let renders = match serde_json::to_string(&want) {",
        "                Ok(s) => s == self.0,",
        "                Err(_) => false,",
        "            };",
        "            parses && renders",
        "        }",
        "",
        "        /// The rendering as the `}`-closed JSON value token (a",
        "        /// `Sample.value` is last in its object).",
        "        fn closed(&self) -> Vec<u8> {",
        "            let mut t = Vec::new();",
        "            t.extend_from_slice(b\"\\\"value\\\":\");",
        "            t.extend_from_slice(self.0.as_bytes());",
        "            t.push(b'}');",
        "            t",
        "        }",
        "",
        "        /// The rendering as the `,`-separated JSON value token (an",
        "        /// `Exemplar.value` precedes `timestampMs`).",
        "        fn separated(&self) -> Vec<u8> {",
        "            let mut t = Vec::new();",
        "            t.extend_from_slice(b\"\\\"value\\\":\");",
        "            t.extend_from_slice(self.0.as_bytes());",
        "            t.push(b',');",
        "            t",
        "        }",
        "",
        "        fn appears(token: &[u8], body: &[u8]) -> bool {",
        "            if token.is_empty() || body.len() < token.len() {",
        "                return false;",
        "            }",
        "            body.windows(token.len()).any(|w| w == token)",
        "        }",
        "",
        "        /// The ONLY search: true iff the rendering appears as a",
        "        /// DELIMITED JSON value token. Never bare — one captured",
        "        /// rendering is a prefix of another (see the #237 table), so a",
        "        /// bare byte check is wrong in BOTH directions.",
        "        pub(crate) fn occurs_in(&self, probe: &WireProbe) -> bool {",
        "            Self::appears(&self.closed(), &probe.0) || Self::appears(&self.separated(), &probe.0)",
        "        }",
        "",
        "        /// This rendering wrapped in caller-chosen text, as a probe —",
        "        /// the delimiter-sensitivity control runs through the same",
        "        /// `occurs_in` the body assertions use.",
        "        pub(crate) fn surrounded_by(&self, left: &str, right: &str) -> WireProbe {",
        "            let mut t = Vec::new();",
        "            t.extend_from_slice(left.as_bytes());",
        "            t.extend_from_slice(self.0.as_bytes());",
        "            t.extend_from_slice(right.as_bytes());",
        "            WireProbe(t)",
        "        }",
        "    }",
        "}",
    ];

    /// `find_subslice`'s byte-frozen body: it keeps its Rule D exemption
    /// (its own `windows`/`position` are the legitimate implementation),
    /// so the exemption must not be repurposable into a bare scanner.
    const FROZEN_FIND_SUBSLICE: &[&str] = &[
        "fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {",
        "    haystack",
        "        .windows(needle.len())",
        "        .position(|window| window == needle)",
        "}",
    ];

    const SPEC_METRICS_RESPONSE: WireScanSpec = WireScanSpec {
        file: "metrics_response.rs",
        frozen_module: FROZEN_WIRE_LITERAL_METRICS_RESPONSE,
        frozen_search_helpers: &[],
        wire_tests: &["duration_seconds_render_on_the_wire_exactly_as_the_reference_emits_them"],
    };

    const SPEC_TRACES_API_LIVE: WireScanSpec = WireScanSpec {
        file: "traces_api_live.rs",
        frozen_module: FROZEN_WIRE_LITERAL_TRACES_API_LIVE,
        frozen_search_helpers: &[("find_subslice", FROZEN_FIND_SUBSLICE)],
        wire_tests: &[
            "duration_seconds_reach_the_wire_exactly_as_the_reference_emits_them",
            "wire_literal_occurs_in_is_delimiter_sensitive",
        ],
    };

    /// Issue #237 guard, structural half: both guarded files scan clean
    /// under Rules A, B, C(freeze), C'(exempt-helper pin), D and E. The
    /// `include_str!` targets resolve at compile time, so a renamed or
    /// moved file is a build error, never a silent skip.
    #[test]
    fn wire_tests_may_only_assert_through_the_wire_literal_helper() {
        assert_eq!(
            scan_wire_assertions(&SPEC_METRICS_RESPONSE, include_str!("metrics_response.rs")),
            vec![]
        );
        assert_eq!(
            scan_wire_assertions(
                &SPEC_TRACES_API_LIVE,
                include_str!("../../tests/traces_api_live.rs")
            ),
            vec![]
        );
    }

    // -- Scanner self-test fixtures: a synthetic spec + source, mutated
    //    one violation at a time. Planted needles are built
    //    arithmetically, never written as source string literals, so the
    //    self-test cannot itself trip Rules A/B on this file.

    const SYN_FILE: &str = "synthetic.rs";

    const SYN_MODULE: &[&str] = &[
        "#[cfg(test)]",
        "mod wire_literal {",
        "    pub(crate) struct WireLiteral(&'static str);",
        "}",
    ];

    const SYN_HELPER: &[&str] = &[
        "fn scan_probe(h: &[u8], n: &[u8]) -> usize {",
        "    h.len()",
        "}",
    ];

    const SYN_SPEC: WireScanSpec = WireScanSpec {
        file: SYN_FILE,
        frozen_module: SYN_MODULE,
        frozen_search_helpers: &[("scan_probe", SYN_HELPER)],
        wire_tests: &["synthetic_wire_test"],
    };

    const SYN_SPEC_NO_TESTS: WireScanSpec = WireScanSpec {
        file: SYN_FILE,
        frozen_module: SYN_MODULE,
        frozen_search_helpers: &[("scan_probe", SYN_HELPER)],
        wire_tests: &[],
    };

    /// A source that satisfies Rules B–E for `SYN_SPEC`. Rule A still
    /// fires on all 24 cells (none of the captured renderings appears
    /// here) — that 24-element baseline is itself the liveness proof
    /// that Rule A can fire at all.
    fn synthetic_lines() -> Vec<String> {
        let mut lines: Vec<String> = Vec::new();
        lines.push(String::from("fn setup() {"));
        lines.push(String::from("}"));
        lines.push(String::new());
        for l in SYN_MODULE {
            lines.push(String::from(*l));
        }
        lines.push(String::new());
        for l in SYN_HELPER {
            lines.push(String::from(*l));
        }
        lines.push(String::new());
        lines.push(String::from("fn synthetic_wire_test() {"));
        lines.push(String::from("    assert!(w.occurs_in(&p));"));
        lines.push(String::from("    assert!(!w.occurs_in(&p));"));
        lines.push(String::from("}"));
        lines
    }

    fn join_lines(lines: &[String]) -> String {
        let mut out = lines.join("\n");
        out.push('\n');
        out
    }

    fn line_index(lines: &[String], exact: &str) -> usize {
        lines
            .iter()
            .position(|l| l == exact)
            .expect("synthetic line present")
    }

    fn syn_baseline() -> Vec<WireScanViolation> {
        let mut out = Vec::new();
        for index in 0..24 {
            out.push(WireScanViolation::LooseCapturedLiteral {
                file: SYN_FILE,
                index,
                quoted: 0,
                constructed: 0,
            });
        }
        out
    }

    fn with_planted(planted: WireScanViolation) -> Vec<WireScanViolation> {
        let mut out = syn_baseline();
        out.push(planted);
        out
    }

    /// Issue #237: one planted source per violation variant, asserted as
    /// exact `Vec` equality (emission order lexer -> A -> B -> C -> C' ->
    /// D -> E), plus a compliant control returning exactly the
    /// 24-element Rule-A baseline and the two lexer cases.
    #[test]
    fn the_wire_assertion_scanner_rejects_planted_violations() {
        // Compliant control: exactly the baseline, nothing else.
        let compliant = synthetic_lines();
        assert_eq!(
            scan_wire_assertions(&SYN_SPEC, &join_lines(&compliant)),
            syn_baseline()
        );

        // Lexer case: a `//` inside a string literal is not a comment
        // start (the literal still closes; nothing else fires).
        let mut lines = synthetic_lines();
        lines.insert(1, String::from("    let url = \"http://example\";"));
        assert_eq!(
            scan_wire_assertions(&SYN_SPEC, &join_lines(&lines)),
            syn_baseline()
        );

        // Lexer case: an escaped quote does not end the literal.
        let mut lines = synthetic_lines();
        lines.insert(1, String::from("    let q = \"a\\\"b\";"));
        assert_eq!(
            scan_wire_assertions(&SYN_SPEC, &join_lines(&lines)),
            syn_baseline()
        );

        // BlockCommentUnsupported — rejected outright, emitted first.
        let mut lines = synthetic_lines();
        lines.insert(1, String::from("    /* opaque */"));
        let mut want = vec![WireScanViolation::BlockCommentUnsupported {
            file: SYN_FILE,
            line: 2,
        }];
        want.append(&mut syn_baseline());
        assert_eq!(scan_wire_assertions(&SYN_SPEC, &join_lines(&lines)), want);

        // BareDecimalLiteral — the needle is built arithmetically.
        let seven = format!("{}", 7.25_f64);
        let mut lines = synthetic_lines();
        let mut planted_line = String::from("    let x = ");
        planted_line.push('"');
        planted_line.push_str(&seven);
        planted_line.push('"');
        planted_line.push(';');
        lines.insert(1, planted_line);
        assert_eq!(
            scan_wire_assertions(&SYN_SPEC, &join_lines(&lines)),
            with_planted(WireScanViolation::BareDecimalLiteral {
                file: SYN_FILE,
                line: 2,
                literal: seven.clone(),
            })
        );

        // MissingWireLiteralModule — the whole block (attribute through
        // closer) removed.
        let mut lines = synthetic_lines();
        let m = line_index(&lines, "mod wire_literal {");
        lines.drain(m - 1..=m + 2);
        assert_eq!(
            scan_wire_assertions(&SYN_SPEC, &join_lines(&lines)),
            with_planted(WireScanViolation::MissingWireLiteralModule { file: SYN_FILE })
        );

        // DuplicateWireLiteralModule.
        let mut lines = synthetic_lines();
        lines.push(String::new());
        for l in SYN_MODULE {
            lines.push(String::from(*l));
        }
        assert_eq!(
            scan_wire_assertions(&SYN_SPEC, &join_lines(&lines)),
            with_planted(WireScanViolation::DuplicateWireLiteralModule { file: SYN_FILE })
        );

        // WireLiteralModuleNotFrozen, planted four ways (#237 plan v7 AC
        // 6d). (i) an attribute directly above the `mod` line:
        let mut lines = synthetic_lines();
        let m = line_index(&lines, "mod wire_literal {");
        lines.insert(m, String::from("#[cfg_attr(all(), inline)]"));
        assert_eq!(
            scan_wire_assertions(&SYN_SPEC, &join_lines(&lines)),
            with_planted(WireScanViolation::WireLiteralModuleNotFrozen {
                file: SYN_FILE,
                line: m + 1,
                found: String::from("#[cfg_attr(all(), inline)]"),
                want: String::from("mod wire_literal {"),
            }),
            "planted attribute directly above the mod line"
        );

        // (ii) the same attribute separated from the mod line by a blank.
        let mut lines = synthetic_lines();
        let m = line_index(&lines, "mod wire_literal {");
        lines.insert(m, String::new());
        lines.insert(m, String::from("#[cfg_attr(all(), inline)]"));
        let got = scan_wire_assertions(&SYN_SPEC, &join_lines(&lines));
        assert!(
            matches!(
                got.last(),
                Some(WireScanViolation::WireLiteralModuleNotFrozen { .. })
            ),
            "blank-separated outer attribute must fail the freeze: {got:?}"
        );

        // (iii) a derive planted inside the module.
        let mut lines = synthetic_lines();
        let st = line_index(&lines, "    pub(crate) struct WireLiteral(&'static str);");
        lines.insert(st, String::from("    #[derive(Debug)]"));
        let got = scan_wire_assertions(&SYN_SPEC, &join_lines(&lines));
        assert!(
            matches!(
                got.last(),
                Some(WireScanViolation::WireLiteralModuleNotFrozen { .. })
            ),
            "planted derive inside the module must fail the freeze: {got:?}"
        );

        // (iv) a one-character body change.
        let mut lines = synthetic_lines();
        let st = line_index(&lines, "    pub(crate) struct WireLiteral(&'static str);");
        lines[st] = String::from("    pub(crate) struct WireLiteral(&'static  str);");
        let got = scan_wire_assertions(&SYN_SPEC, &join_lines(&lines));
        assert!(
            matches!(
                got.last(),
                Some(WireScanViolation::WireLiteralModuleNotFrozen { .. })
            ),
            "a one-character module change must fail the freeze: {got:?}"
        );

        // ExemptHelperBodyChanged.
        let mut lines = synthetic_lines();
        let h = line_index(&lines, "    h.len()");
        lines[h] = String::from("    h.len() + 1");
        assert_eq!(
            scan_wire_assertions(&SYN_SPEC, &join_lines(&lines)),
            with_planted(WireScanViolation::ExemptHelperBodyChanged {
                file: SYN_FILE,
                helper: "scan_probe",
                line: h + 1,
            })
        );

        // UnguardedSearchCall — a derived-needle family call.
        let mut lines = synthetic_lines();
        lines.insert(1, String::from("    let _ = hay.contains(&needle);"));
        assert_eq!(
            scan_wire_assertions(&SYN_SPEC, &join_lines(&lines)),
            with_planted(WireScanViolation::UnguardedSearchCall {
                file: SYN_FILE,
                line: 2,
                text: String::from("let _ = hay.contains(&needle);"),
            })
        );

        // MissingWireAssertion — the negative half deleted.
        let lines: Vec<String> = synthetic_lines()
            .into_iter()
            .filter(|l| l != "    assert!(!w.occurs_in(&p));")
            .collect();
        assert_eq!(
            scan_wire_assertions(&SYN_SPEC, &join_lines(&lines)),
            with_planted(WireScanViolation::MissingWireAssertion {
                file: SYN_FILE,
                test: "synthetic_wire_test",
                what: WHAT_NEGATIVE,
            })
        );

        // MissingWireAssertion — the positive half deleted.
        let lines: Vec<String> = synthetic_lines()
            .into_iter()
            .filter(|l| l != "    assert!(w.occurs_in(&p));")
            .collect();
        assert_eq!(
            scan_wire_assertions(&SYN_SPEC, &join_lines(&lines)),
            with_planted(WireScanViolation::MissingWireAssertion {
                file: SYN_FILE,
                test: "synthetic_wire_test",
                what: WHAT_POSITIVE,
            })
        );

        // TableCardinalityMismatch — emitted ALONE (short-circuit).
        assert_eq!(
            scan_with_table(
                &SYN_SPEC,
                &join_lines(&synthetic_lines()),
                &REFERENCE_DURATION_SECONDS[..11]
            ),
            vec![WireScanViolation::TableCardinalityMismatch {
                file: SYN_FILE,
                rows: 11,
                want: 12,
            }]
        );

        // EmptyWireTestList — Rule E cannot be emptied into a pass.
        assert_eq!(
            scan_wire_assertions(&SYN_SPEC_NO_TESTS, &join_lines(&synthetic_lines())),
            with_planted(WireScanViolation::EmptyWireTestList { file: SYN_FILE })
        );
    }
}
