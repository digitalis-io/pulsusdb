//! Issue #252 AC3a/AC3b/AC8: the hermetic replay of the committed
//! reference capture (`tests/golden/traces_metrics/log2_reference_capture.json`,
//! `grafana/tempo:3.0.2@sha256:cda87c21…`, 2026-08-05).
//!
//! Three separate claims are pinned here, and they are different in kind:
//!
//! - **AC3a — `histogram_over_time` MATCHES.** Our bucket rule
//!   ([`pulsus_read::traces::log2_histogram`]) applied to each corpus's
//!   durations reproduces the reference's emitted `__bucket` set, its
//!   per-bucket tallies, and — the part a "better ladder" could never
//!   satisfy — the buckets it emitted NO series for.
//! - **AC8 — the emitted ORDER matches**, through the production
//!   comparator (`sortResponse`,
//!   `modules/frontend/combiner/metrics_query_range.go:245-266 @ v3.0.2`),
//!   which orders on the label value RENDERED AS GO'S `%g` — the
//!   `AnyValue.String()`/`CompactTextString` path, NOT the protojson
//!   body. Two capture corpora exist solely to hold that distinction
//!   down; see the order test.
//! - **AC3b — `quantile_over_time` DIVERGES, and we have characterised
//!   the divergence correctly.** A test-only port of the reference's
//!   `Log2QuantileWithBucket` (`pkg/traceql/engine_metrics.go:2058-2120
//!   @ v3.0.2`) reproduces every captured reference quantile from the
//!   captured tallies. This is a characterisation ORACLE, deliberately
//!   NOT a code path: PulsusDB serves `quantile_over_time` from
//!   `quantilesTDigest` over the raw durations (ledger row
//!   `2026-08-05-traceql-quantile-over-time-tdigest`). Its job is to make
//!   the ledger's claim — "theirs is an upper bound consistent with the
//!   same histogram" — checkable rather than rhetorical.
//!
//! **Why AC3b is exact on some branches and bounded on others.** The
//! reference's exact-hit branch returns `b.Max`, a bucket label: an
//! integer power of two divided by `1e9`, one rounding, no transcendental
//! — both languages are exact there, so those witnesses are compared with
//! `to_bits()`. The interpolation branch composes `math.Log2` and
//! `math.Pow`, for which Rust and Go share no correctly-rounded
//! guarantee, so those are compared at relative error ≤ 1e-12. That bound
//! is ~2 orders above the worst plausible libm composition error (~7e-15)
//! and ~10 orders BELOW the smallest behavioural error it must catch (an
//! off-by-one in `max_samples` on a 20-span corpus moves the result by
//! `2^(1/20)`, i.e. 3.5%). Both mutants were run and both fail; see the
//! issue notes.

use std::collections::BTreeMap;

use pulsus_read::traces::exec::sort_series_like_the_reference;
use pulsus_read::traces::log2_histogram::{bucket_seconds, log2_bucketize_ns};
use pulsus_read::{MetricLabel, MetricLabelValue, TraceMetricSeries};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Capture {
    corpora: Vec<Corpus>,
}

#[derive(Debug, Deserialize)]
struct Corpus {
    name: String,
    durations_ns: Vec<i64>,
    /// The buckets IN THE ORDER the reference returned them, each with
    /// the `doubleValue` text read off its HTTP body.
    emitted_buckets: Vec<CapturedBucket>,
    absent_buckets_ns: Vec<u64>,
    count_over_time: usize,
    quantiles: Vec<CapturedQuantile>,
}

#[derive(Debug, Deserialize)]
struct CapturedBucket {
    bucket_ns: u64,
    seconds: f64,
    count: u64,
    wire_text: String,
}

#[derive(Debug, Deserialize)]
struct CapturedQuantile {
    p: f64,
    value: f64,
    branch: String,
}

fn capture() -> Capture {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden/traces_metrics/log2_reference_capture.json");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {path:?}: {e}"))
}

/// Our side's tallies for a corpus, computed the way the engine does:
/// bucket each duration, drop the ones the reference drops, count.
fn our_tallies(durations_ns: &[i64]) -> BTreeMap<u64, u64> {
    let mut out: BTreeMap<u64, u64> = BTreeMap::new();
    for d in durations_ns {
        if let Some(bucket) = log2_bucketize_ns(*d) {
            *out.entry(bucket).or_default() += 1;
        }
    }
    out
}

#[test]
fn our_bucket_rule_reproduces_every_captured_reference_histogram() {
    for corpus in capture().corpora {
        let ours = our_tallies(&corpus.durations_ns);
        let theirs: BTreeMap<u64, u64> = corpus
            .emitted_buckets
            .iter()
            .map(|b| (b.bucket_ns, b.count))
            .collect();
        assert_eq!(
            ours, theirs,
            "{}: bucket set AND tallies must match the reference exactly",
            corpus.name
        );
        // The label rendering, bit-exact.
        for b in &corpus.emitted_buckets {
            assert_eq!(
                bucket_seconds(b.bucket_ns).to_bits(),
                b.seconds.to_bits(),
                "{}: 2^? = {} ns renders as {}",
                corpus.name,
                b.bucket_ns,
                b.seconds
            );
        }
        // Membership is occurrence-only: a bucket the reference emitted
        // NO series for must not appear on our side either. A "better
        // fixed ladder" fails exactly here.
        for absent in &corpus.absent_buckets_ns {
            assert!(
                !ours.contains_key(absent),
                "{}: {absent} ns is an EMPTY bucket — the reference emits no series for it",
                corpus.name
            );
        }
        // Tallies are plain counts, so they SUM to the surviving span
        // count. The cumulative form could not satisfy this.
        let sum: u64 = ours.values().sum();
        let survivors = corpus.durations_ns.iter().filter(|d| **d >= 2).count() as u64;
        assert_eq!(
            sum, survivors,
            "{}: tallies are not cumulative",
            corpus.name
        );
        // …and `count_over_time` still counts the spans the histogram
        // dropped (the sub-2ns rule drops them from the SERIES, not from
        // the corpus).
        assert_eq!(
            corpus.count_over_time,
            corpus.durations_ns.len(),
            "{}: count_over_time counts every span",
            corpus.name
        );
    }
}

/// AC8, with no exemptions: the production comparator must reproduce the
/// order the reference actually emitted, for EVERY captured corpus.
///
/// Two of the corpora exist only for this test, and both were measured
/// on the container rather than derived:
///
/// - **`mix1024`** (`2^10` + `2^20 ns`) — the reference emits
///   `0.001048576` then `0.000001024`. Sorting on the WIRE text would
///   emit them the other way round, because protojson renders `2^10 ns`
///   as `0.000001024` while the sort key renders it `1.024e-06`.
/// - **`mix16k`** (`2^14` + `2^20 ns`, i.e. a 16 µs span beside a 1 ms
///   span — an ordinary corpus, not an exotic one) — the reference emits
///   `0.001048576` then `0.000016384`. Here `serde_json` and protojson
///   AGREE on `0.000016384` and it is Go's `%g` that differs
///   (`1.6384e-05`), so a comparator built on our own JSON rendering
///   gets this backwards.
///
/// Those two are why [`sort_series_like_the_reference`] compares Go's
/// `%g` rather than the wire text; without them the wrong comparator
/// passes every other corpus here.
#[test]
fn the_production_comparator_reproduces_the_reference_series_order() {
    let mut discriminating: Vec<String> = Vec::new();
    let mut reference_wire_discriminating: Vec<String> = Vec::new();
    for corpus in capture().corpora {
        let mut series: Vec<TraceMetricSeries> = our_tallies(&corpus.durations_ns)
            .into_iter()
            // The SQL hands these over ascending by bucket_ns, which is
            // NOT the order the reference emits.
            .map(|(bucket_ns, n)| TraceMetricSeries {
                labels: vec![MetricLabel::double("__bucket", bucket_seconds(bucket_ns))],
                samples: vec![(0, n as f64)],
                exemplars: vec![],
            })
            .collect();
        sort_series_like_the_reference(&mut series);
        let ours: Vec<u64> = series
            .iter()
            .map(|s| {
                let MetricLabelValue::Double(seconds) = s.labels[0].value else {
                    panic!("__bucket is a double");
                };
                (seconds * 1e9).round() as u64
            })
            .collect();
        let theirs: Vec<u64> = corpus.emitted_buckets.iter().map(|b| b.bucket_ns).collect();
        assert_eq!(
            ours, theirs,
            "{}: our series order must equal the order the reference emitted",
            corpus.name
        );

        // …and the order a comparator keyed on OUR OWN wire text would
        // have produced, counted per corpus so a corpus that cannot tell
        // the two rules apart is visibly not carrying the claim.
        let mut by_our_wire_text = theirs.clone();
        by_our_wire_text.sort_by_key(|ns| {
            serde_json::to_string(&bucket_seconds(*ns)).expect("finite bucket label")
        });
        if by_our_wire_text != theirs {
            discriminating.push(corpus.name.clone());
        }

        // …and the order a comparator keyed on the REFERENCE's own wire
        // text would have produced. That one is not the sort key either
        // — `sortResponse` compares `AnyValue.String()` (Go `%g`), not
        // the protojson body — and `mix1024` is the corpus that proves
        // it: protojson writes `2^10 ns` as `0.000001024`, which sorts
        // FIRST, while the reference emitted it SECOND.
        let mut by_their_wire_text: Vec<u64> = theirs.clone();
        let wire: BTreeMap<u64, String> = corpus
            .emitted_buckets
            .iter()
            .map(|b| (b.bucket_ns, b.wire_text.clone()))
            .collect();
        by_their_wire_text.sort_by_key(|ns| wire[ns].clone());
        if by_their_wire_text != theirs {
            reference_wire_discriminating.push(corpus.name.clone());
        }
    }
    assert_eq!(
        discriminating,
        vec!["mix16k".to_string()],
        "exactly one captured corpus distinguishes the reference's order from a comparator \
         keyed on OUR wire text; without it, the wrong comparator passes this test"
    );
    assert_eq!(
        reference_wire_discriminating,
        vec!["mix1024".to_string(), "mix16k".to_string()],
        "both order corpora show the reference does NOT sort on its own wire text — which is \
         why the sort key is Go's %g and not a rendering of the response body"
    );
}

/// AC9, recorded rather than filed: the JSON float text, both sides,
/// measured. Same value, same parse, different text — cosmetic under the
/// consumer-impact rule, but it is NOT the sort key (see
/// `the_production_comparator_reproduces_the_reference_series_order`),
/// so it changes no ordering.
#[test]
fn the_wire_float_text_divergence_is_exactly_the_recorded_one() {
    let mut differing: Vec<(u64, String, String)> = Vec::new();
    for corpus in capture().corpora {
        for b in &corpus.emitted_buckets {
            let ours =
                serde_json::to_string(&bucket_seconds(b.bucket_ns)).expect("finite bucket label");
            if ours != b.wire_text {
                differing.push((b.bucket_ns, b.wire_text.clone(), ours));
            }
        }
    }
    differing.sort();
    differing.dedup();
    // protojson uses `encoding/json`'s rule (exponent form iff
    // |v| < 1e-6 or >= 1e21, leading zero stripped from the exponent);
    // ryu switches at a different threshold. Over every bucket the
    // capture covers, that is one bucket: 2^10 ns.
    assert_eq!(
        differing,
        vec![(1024u64, "0.000001024".to_string(), "1.024e-6".to_string())],
        "recorded wire-text divergences"
    );
}

// ---------------------------------------------------------------------
// AC3b — the characterisation oracle. NOT a code path.
// ---------------------------------------------------------------------

/// One tally as the reference's `HistogramBucket` carries it: the
/// bucket's upper bound in float seconds and its count.
#[derive(Debug, Clone, Copy)]
struct Bucket {
    max: f64,
    count: u64,
}

/// Which branch of `Log2QuantileWithBucket` produced a value — the
/// exact-hit `return b.Max` or the exponential interpolation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Branch {
    Exact,
    Interpolated,
}

impl Branch {
    fn as_str(self) -> &'static str {
        match self {
            Branch::Exact => "exact",
            Branch::Interpolated => "interpolated",
        }
    }
}

/// A line-for-line port of `Log2QuantileWithBucket`
/// (`pkg/traceql/engine_metrics.go:2058-2120 @ v3.0.2`). Deliberately
/// NOT "improved": every early return, the `maxSamples == 0 → 1` floor
/// and the `minV = maxV - 1` no-prior-bucket case are the reference's.
///
/// `buckets` must be ascending by `max` with every `count > 0` — the
/// reference guarantees both upstream, not inside this walk:
/// `Histogram.Record` (`:1700-1712`) appends a bucket only on first
/// observation, `HistogramAggregator.Combine` (`:1864`, zero-skip at
/// `:1899`) drops zero-valued samples, and `Results` (`:1930`,
/// per-interval sort at `:1969`) sorts each slice before the walk. So
/// slice-adjacent entries are adjacent OCCUPIED buckets, and the
/// interpolation spans skipped powers of two without knowing it has —
/// which is why the `w252` `p = 0.75` witness lands exactly on `2^31`, a
/// bucket with no series at all.
fn log2_quantile(p: f64, buckets: &[Bucket]) -> (f64, Branch) {
    if p.is_nan() || !(0.0..=1.0).contains(&p) || buckets.is_empty() {
        return (0.0, Branch::Exact);
    }
    let total: u64 = buckets.iter().map(|b| b.count).sum();
    if total == 0 {
        return (0.0, Branch::Exact);
    }
    let mut max_samples = (p * total as f64).ceil() as u64;
    if max_samples == 0 {
        max_samples = 1;
    }
    let mut consumed: u64 = 0;
    let mut idx = 0usize;
    for (i, b) in buckets.iter().enumerate() {
        idx = i;
        if consumed + b.count > max_samples {
            break;
        }
        consumed += b.count;
        if consumed == max_samples {
            return (b.max, Branch::Exact);
        }
    }
    let interp = (max_samples - consumed) as f64 / buckets[idx].count as f64;
    let max_v = buckets[idx].max.log2();
    let min_v = if idx > 0 {
        buckets[idx - 1].max.log2()
    } else {
        max_v - 1.0
    };
    (
        2f64.powf(min_v + (max_v - min_v) * interp),
        Branch::Interpolated,
    )
}

#[test]
fn the_oracle_reproduces_every_captured_reference_quantile() {
    let mut witnesses = 0usize;
    for corpus in capture().corpora {
        let buckets: Vec<Bucket> = corpus
            .emitted_buckets
            .iter()
            .map(|b| Bucket {
                max: b.seconds,
                count: b.count,
            })
            .collect();
        for q in &corpus.quantiles {
            let (got, branch) = log2_quantile(q.p, &buckets);
            assert_eq!(
                branch.as_str(),
                q.branch,
                "{}: p={} took the {:?} branch, capture says {}",
                corpus.name,
                q.p,
                branch,
                q.branch
            );
            match branch {
                // Exact by construction in both languages: the return is
                // a bucket label, an integer power of two over 1e9.
                Branch::Exact => assert_eq!(
                    got.to_bits(),
                    q.value.to_bits(),
                    "{}: p={} exact-hit must be bit-identical (got {got}, want {})",
                    corpus.name,
                    q.p,
                    q.value
                ),
                // log2/powf: bounded, never loosened. A miss here is a
                // finding to report, not a bound to widen.
                Branch::Interpolated => {
                    let rel = (got - q.value).abs() / q.value.abs();
                    assert!(
                        rel <= 1e-12,
                        "{}: p={} interpolated to {got}, reference {} (relative error {rel:e} > 1e-12)",
                        corpus.name,
                        q.p,
                        q.value
                    );
                }
            }
            witnesses += 1;
        }
    }
    // The ledger row rests on these; a capture that quietly lost its
    // quantiles would otherwise pass vacuously.
    assert_eq!(witnesses, 19, "every captured quantile is replayed");
}

#[test]
fn the_oracle_returns_the_references_degenerate_answers() {
    // The `p` out of range / NaN / empty-slice / zero-total early
    // returns, all `0` in the reference.
    let buckets = [Bucket {
        max: 0.536870912,
        count: 20,
    }];
    for p in [-0.1, 1.1, f64::NAN] {
        assert_eq!(log2_quantile(p, &buckets).0.to_bits(), 0.0f64.to_bits());
    }
    assert_eq!(log2_quantile(0.5, &[]).0.to_bits(), 0.0f64.to_bits());
    assert_eq!(
        log2_quantile(0.5, &[Bucket { max: 1.0, count: 0 }])
            .0
            .to_bits(),
        0.0f64.to_bits()
    );
    // The `maxSamples == 0 → 1` floor: p = 0 must read one sample, not
    // zero, so it takes the interpolation branch rather than returning
    // the first bucket's label.
    let (v, branch) = log2_quantile(0.0, &buckets);
    assert_eq!(branch, Branch::Interpolated);
    assert!(v < 0.536870912, "p=0 interpolates below the bucket max");
}

#[test]
fn the_references_quantile_is_a_function_of_the_bucket_not_of_the_durations() {
    // The ledger row's load-bearing claim, checked against the capture:
    // three corpora at 280 / 300 / 520 ms — an 86% spread — produce
    // byte-identical reference output at every p, because all three
    // occupy the single bucket 2^29 ns.
    let cap = capture();
    let of = |name: &str| -> Vec<(f64, u64)> {
        cap.corpora
            .iter()
            .find(|c| c.name == name)
            .unwrap_or_else(|| panic!("corpus {name}"))
            .quantiles
            .iter()
            .map(|q| (q.p, q.value.to_bits()))
            .collect()
    };
    let u280 = of("u280");
    assert_eq!(u280, of("u300"));
    assert_eq!(u280, of("u520"));
    assert!(!u280.is_empty());
    // And the true p99 of the 300 ms corpus is 0.3 s, which the
    // reference reports as 0.536870912 s — 79% high. Our own value is
    // pinned live (`traces_metrics_live.rs`), against real tDigest.
    let p99 = u280
        .iter()
        .find(|(p, _)| *p == 0.99)
        .expect("p99 witness")
        .1;
    assert_eq!(p99, 0.536870912f64.to_bits());
}
