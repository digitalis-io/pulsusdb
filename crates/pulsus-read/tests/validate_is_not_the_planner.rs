//! Issue #328 D5′: the behavioural half of the validator/planner
//! separation claim.
//!
//! The cargo cycle proves ISOLATION — `pulsus-traceql` cannot name
//! `PlanError`/`plan_search`/`SearchCtx`, so the validator cannot call
//! or branch on the planner. What the cycle cannot prove is SEPARATION:
//! a planner-only rule could still be re-implemented by hand inside
//! `validate`. This suite (hermetic — `pulsus-read` can name both
//! sides) is the behavioural allowlist standing against that:
//!
//! * every `tempo: "accept"` vector in `validate-vectors.json` must
//!   validate `Ok(())` — the reference's validator accepts them, so a
//!   creeping planner rule that rejects any of them reddens here; and
//! * the vectors must witness EVERY `PlanError` variant with at least
//!   one accept row the planner rejects with exactly that variant — the
//!   proof that "accepted by the validator" and "accepted by the
//!   planner" are genuinely different sets, per variant.
//!
//! **Residual, stated (plan v3 D5′):** a copied planner rule NARROWER
//! than every witness (e.g. one rejecting only `{} | rate() | topk(3)`)
//! still passes this suite. What catches it is the whole-corpus half in
//! `pulsus-traceql/tests/validate_corpus.rs` (every registry probe and
//! accept-corpus case the recorded Tempo verdict accepts must validate
//! `Ok`) plus the `VALIDATE_RULES` citation table, which forces a new
//! variant and a reference citation. There is no compile-time mechanism
//! for this and none is claimed.

use std::path::PathBuf;

use serde::Deserialize;

use pulsus_read::{
    MetricsCtx, MetricsParams, SearchCtx, SearchParams, SpanFilterCtx, TracePlanError, plan_search,
    plan_trace_metrics,
};
use pulsus_traceql::{parse, validate};

#[derive(Debug, Deserialize)]
struct Vectors {
    vectors: Vec<Vector>,
}

#[derive(Debug, Deserialize)]
struct Vector {
    id: String,
    query: String,
    tempo: String,
    /// A ledgered pulsus-vs-tempo divergence (the nil-spelling
    /// conflation rows): exempt from the allowlist — their evidence is
    /// the vectors↔ledger link in `validate_corpus.rs`, not this suite.
    #[serde(default)]
    divergence: Option<String>,
}

fn load_vectors() -> Vec<Vector> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../pulsus-traceql/tests/conformance/validate-vectors.json");
    let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    serde_json::from_str::<Vectors>(&raw)
        .expect("vectors JSON")
        .vectors
}

fn search_ctx<'a>(spans: &'a str, attrs: &'a str) -> (SpanFilterCtx<'a>, SearchParams) {
    (
        SpanFilterCtx {
            spans_table: spans,
            attrs_table: attrs,
        },
        SearchParams {
            start_ns: 1_000_000_000,
            end_ns: 2_000_000_000,
            limit: 20,
            spss: 3,
        },
    )
}

/// Every expression the reference's validator accepts must validate
/// `Ok` here — whatever any planner thinks of it.
#[test]
fn every_tempo_accepted_vector_validates_ok() {
    let mut checked = 0usize;
    for v in load_vectors() {
        if v.tempo != "accept" || v.divergence.is_some() {
            continue;
        }
        let query = parse(&v.query).unwrap_or_else(|e| panic!("{}: must parse: {e}", v.id));
        assert_eq!(
            validate(&query),
            Ok(()),
            "{}: {:?} is reference-accepted; a validator rejection here is planner strictness \
             leaking into the route-independent pass",
            v.id,
            v.query
        );
        checked += 1;
    }
    assert!(checked >= 30, "only {checked} accept vectors checked");
}

/// The separation witnesses: for EVERY `PlanError` variant, at least one
/// reference-accepted (and validator-accepted) vector the planner
/// rejects with exactly that variant. The `match` on the witnessed
/// variants is exhaustive over the enum, so a new `PlanError` variant
/// fails this test until a witness exists.
#[test]
fn every_plan_error_variant_is_witnessed_by_an_accepted_vector() {
    let vectors = load_vectors();
    let accepted: Vec<&Vector> = vectors
        .iter()
        .filter(|v| v.tempo == "accept" && v.divergence.is_none())
        .collect();

    let mut unsupported_field = 0usize;
    let mut type_mismatch = 0usize;
    let mut point_cap = 0usize;

    let (filter, params) = search_ctx("pulsus.trace_spans", "pulsus.trace_attrs_idx");
    let ctx = SearchCtx {
        filter,
        max_candidates: 1000,
        max_series: 500,
        distributed: false,
    };
    let mctx = MetricsCtx {
        filter,
        scan_budget_rows: 1_000_000,
        max_series: 500,
        distributed: false,
        skip_unavailable_shards: false,
    };
    // A range that resolves far more buckets than the metrics point cap,
    // so any valid metrics expression trips `MetricsPointCap` statically.
    let huge = MetricsParams {
        start_ns: 0,
        end_ns: 4_000_000_000_000_000_000,
        step_ms: 1_000,
        exemplars: None,
    };

    for v in &accepted {
        let query = parse(&v.query).expect("accept vectors parse");
        assert_eq!(
            validate(&query),
            Ok(()),
            "{}: accept vectors validate",
            v.id
        );
        match plan_search(&query, &params, &ctx) {
            Err(TracePlanError::UnsupportedField(_)) => unsupported_field += 1,
            Err(TracePlanError::TypeMismatch(_)) => type_mismatch += 1,
            Err(TracePlanError::MetricsPointCap { .. }) => point_cap += 1,
            Ok(_) => {}
        }
        if let Err(TracePlanError::MetricsPointCap { .. }) =
            plan_trace_metrics(&query, &huge, &mctx)
        {
            point_cap += 1;
        }
    }

    // The exhaustive destructure over the enum (`filter.rs:75-82`): a
    // new variant breaks this block until it is decided here.
    let witness_of = |e: &TracePlanError| match e {
        TracePlanError::UnsupportedField(_) => "unsupported-field",
        TracePlanError::TypeMismatch(_) => "type-mismatch",
        TracePlanError::MetricsPointCap { .. } => "metrics-point-cap",
    };
    let _ = witness_of;

    assert!(
        unsupported_field >= 1,
        "no accept vector witnesses PlanError::UnsupportedField — expected e.g. \
         `{{}} | by(event:name)` (reference-accepted, plan_search-rejected)"
    );
    assert!(
        type_mismatch >= 1,
        "no accept vector witnesses PlanError::TypeMismatch — expected e.g. `{{}} | rate()` \
         (reference-accepted, plan_search rejects metrics stages on search)"
    );
    assert!(
        point_cap >= 1,
        "no accept vector witnesses PlanError::MetricsPointCap — expected any valid metrics \
         expression against the huge range"
    );
}
