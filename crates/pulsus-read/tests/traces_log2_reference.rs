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
//! - **AC8 — the emitted ORDER deliberately DIVERGES.** We emit
//!   ascending by bucket; the reference emits lexicographic on a Go
//!   `%g` rendering of the label (`sortResponse` compares
//!   `AnyValue.String()`), which is neither numeric nor the order of its
//!   own JSON body. Owner ruling 2026-08-05, ledgered as
//!   `2026-08-05-traceql-histogram-series-order`. Both orders are pinned
//!   per corpus, with `mix16k`/`mixladder` as the witnesses.
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

use pulsus_read::traces::exec::sort_histogram_series_by_bucket_ascending;
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

/// The reference's emitted series order, corpus by corpus, as an
/// EXPLICIT expected sequence — not a property of the capture restated
/// from it.
///
/// This table is the thing the ledger row
/// `2026-08-05-traceql-histogram-series-order` rests on. Written out in
/// full so a change on the reference's side fails here: a summary
/// assertion ("these corpora are non-ascending") would pass for any
/// other non-ascending order and would therefore pin nothing.
///
/// Read it against `%g`, which is the rule producing it — lexicographic
/// on `strconv.FormatFloat(v, 'g', -1, 64)`:
///
/// - `mix252`: `0.001048576` < `2e-09` < `4e-09`;
/// - `mix1024`: `0.001048576` < `1.024e-06`;
/// - `mix16k`: `0.001048576` < `1.6384e-05`;
/// - `mixladder`: `0.001048576` < `1.024e-06` < `1.073741824` < `1.6384e-05`;
/// - `expform`: `2.048e-06` < `4.096e-06` < `8.192e-06` (ascending here,
///   by coincidence of the digits);
/// - the single-bucket and two-ascending-bucket corpora cannot
///   distinguish any rule and are here for completeness.
const REFERENCE_EMITTED_ORDER: &[(&str, &[u64])] = &[
    ("w252", &[1 << 29, 1 << 33]),
    ("mix252", &[1 << 20, 1 << 1, 1 << 2]),
    ("u280", &[1 << 29]),
    ("u300", &[1 << 29]),
    ("u520", &[1 << 29]),
    ("tiny252", &[1 << 10, 1 << 40]),
    ("mix1024", &[1 << 20, 1 << 10]),
    ("mix16k", &[1 << 20, 1 << 14]),
    ("mixladder", &[1 << 20, 1 << 10, 1 << 30, 1 << 14]),
    ("expform", &[1 << 11, 1 << 12, 1 << 13]),
];

/// The order each corpus would come back in if the reference sorted on
/// its OWN JSON body instead of on `AnyValue.String()` — pinned
/// explicitly for the same reason as the table above, and different from
/// it for `mix1024`, `mix16k` and `mixladder`. Without this, someone
/// re-deriving the rule from `wire_text` could conclude we might have
/// matched the reference by sorting on the response.
const IF_SORTED_ON_THE_REFERENCE_WIRE_TEXT: &[(&str, &[u64])] = &[
    ("w252", &[1 << 29, 1 << 33]),
    ("mix252", &[1 << 20, 1 << 1, 1 << 2]),
    ("u280", &[1 << 29]),
    ("u300", &[1 << 29]),
    ("u520", &[1 << 29]),
    ("tiny252", &[1 << 10, 1 << 40]),
    // `0.000001024` < `0.001048576` — the opposite of what it emitted.
    ("mix1024", &[1 << 10, 1 << 20]),
    // `0.000016384` < `0.001048576` — likewise.
    ("mix16k", &[1 << 14, 1 << 20]),
    ("mixladder", &[1 << 10, 1 << 14, 1 << 20, 1 << 30]),
    ("expform", &[1 << 11, 1 << 12, 1 << 13]),
];

fn expected(table: &[(&str, &[u64])], name: &str) -> Vec<u64> {
    table
        .iter()
        .find(|(n, _)| *n == name)
        .unwrap_or_else(|| panic!("corpus {name} has no pinned order — add it to the table"))
        .1
        .to_vec()
}

/// AC8 under the owner's 2026-08-05 ruling: **our** order is ascending by
/// bucket, and the reference's is pinned beside it as the ledgered
/// divergence. Nothing is exempted and neither side is derived from the
/// other, so a change on either fails here.
///
/// `mix16k` is the witness the ledger row rests on: a 16 µs span beside a
/// 1 ms span — an ordinary corpus, not an exotic one — for which the
/// reference emits `2^20` BEFORE `2^14`. `mixladder` is the four-bucket
/// form docs/api.md §4.4.1 quotes: spans at 1 µs, 16 µs, 1 ms and 1 s
/// come back `1 ms, 1 µs, 1 s, 16 µs`.
#[test]
fn we_emit_ascending_by_bucket_and_the_reference_order_is_pinned_beside_it() {
    let corpora = capture().corpora;
    // Every captured corpus is pinned, and every pin has a corpus —
    // otherwise a corpus could be added or dropped without the table
    // noticing.
    let captured: Vec<&str> = corpora.iter().map(|c| c.name.as_str()).collect();
    let pinned: Vec<&str> = REFERENCE_EMITTED_ORDER.iter().map(|(n, _)| *n).collect();
    assert_eq!(captured, pinned, "capture membership vs the pinned table");
    let wire_pinned: Vec<&str> = IF_SORTED_ON_THE_REFERENCE_WIRE_TEXT
        .iter()
        .map(|(n, _)| *n)
        .collect();
    assert_eq!(
        captured, wire_pinned,
        "capture membership vs the wire table"
    );

    for corpus in &corpora {
        let theirs: Vec<u64> = corpus.emitted_buckets.iter().map(|b| b.bucket_ns).collect();

        // THEIRS: the exact sequence, pinned. This is what makes the
        // ledger row checkable rather than asserted.
        assert_eq!(
            theirs,
            expected(REFERENCE_EMITTED_ORDER, &corpus.name),
            "{}: the reference's emitted series order",
            corpus.name
        );

        // …and the order its own JSON body would have given, pinned too,
        // so the mechanism claim is a sequence and not a name list.
        let wire: BTreeMap<u64, String> = corpus
            .emitted_buckets
            .iter()
            .map(|b| (b.bucket_ns, b.wire_text.clone()))
            .collect();
        let mut by_their_wire = theirs.clone();
        by_their_wire.sort_by_key(|ns| wire[ns].clone());
        assert_eq!(
            by_their_wire,
            expected(IF_SORTED_ON_THE_REFERENCE_WIRE_TEXT, &corpus.name),
            "{}: the order the reference's OWN wire text would give",
            corpus.name
        );

        // OURS: ascending, always — framed the way the engine frames,
        // through the production sort.
        let mut series: Vec<TraceMetricSeries> = our_tallies(&corpus.durations_ns)
            .into_iter()
            .map(|(bucket_ns, n)| TraceMetricSeries {
                labels: vec![MetricLabel::double("__bucket", bucket_seconds(bucket_ns))],
                samples: vec![(0, n as f64)],
                exemplars: vec![],
            })
            .collect();
        sort_histogram_series_by_bucket_ascending(&mut series);
        let ours: Vec<u64> = series
            .iter()
            .map(|s| {
                let MetricLabelValue::Double(seconds) = s.labels[0].value else {
                    panic!("__bucket is a double");
                };
                (seconds * 1e9).round() as u64
            })
            .collect();
        let mut ascending = theirs.clone();
        ascending.sort_unstable();
        assert_eq!(
            ours, ascending,
            "{}: PulsusDB emits histogram series ascending by bucket",
            corpus.name
        );
        assert!(
            ours.windows(2).all(|w| w[0] < w[1]),
            "{}: strictly ascending, {ours:?}",
            corpus.name
        );
    }

    // The divergence, summarised from the pinned sequences — readable,
    // and now backed by them rather than standing in for them.
    let diverging: Vec<&str> = REFERENCE_EMITTED_ORDER
        .iter()
        .filter(|(_, order)| {
            let mut asc = order.to_vec();
            asc.sort_unstable();
            asc != *order
        })
        .map(|(n, _)| *n)
        .collect();
    assert_eq!(
        diverging,
        vec!["mix252", "mix1024", "mix16k", "mixladder"],
        "the corpora whose reference order is NOT ascending — the ledgered divergence's \
         witnesses"
    );
    let wire_diverging: Vec<&str> = REFERENCE_EMITTED_ORDER
        .iter()
        .zip(IF_SORTED_ON_THE_REFERENCE_WIRE_TEXT)
        .filter(|((_, emitted), (_, by_wire))| emitted != by_wire)
        .map(|((n, _), _)| *n)
        .collect();
    assert_eq!(
        wire_diverging,
        vec!["mix1024", "mix16k", "mixladder"],
        "the corpora the reference orders differently from its own response body — the \
         evidence that its sort key is Go's %g on AnyValue.String(), not the JSON text"
    );
}

/// AC9, recorded rather than filed: the JSON float text, both sides,
/// measured on every member rather than derived for three of them —
/// which is why the `expform` corpus (`2^11`, `2^12`, `2^13`) is in the
/// capture at all.
///
/// Same value, same parse, different text: protojson uses
/// `encoding/json`'s rule (exponent form iff `|v| < 1e-6` or `>= 1e21`)
/// and `serde_json`/ryu switches at a different threshold, so the four
/// buckets `2^10 .. 2^13 ns` render plain-decimal there and in exponent
/// form here. Cosmetic under the consumer-impact rule, and it is NOT the
/// sort key — see
/// `we_emit_ascending_by_bucket_and_the_reference_order_is_pinned_beside_it`
/// — so it has no ordering consequence on either side.
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
    // Over every bucket the capture covers, the divergence is exactly
    // the exponent-form band `2^10 .. 2^13 ns` — all four measured, none
    // inferred.
    assert_eq!(
        differing,
        vec![
            (1024u64, "0.000001024".to_string(), "1.024e-6".to_string()),
            (2048, "0.000002048".to_string(), "2.048e-6".to_string()),
            (4096, "0.000004096".to_string(), "4.096e-6".to_string()),
            (8192, "0.000008192".to_string(), "8.192e-6".to_string()),
        ],
        "recorded wire-text divergences: the whole of 2^10..2^13, each measured off the \
         reference's HTTP body, and nothing else in the capture"
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
