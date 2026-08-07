//! Issue #248: the accept/reject surface of an `ip()` label filter,
//! measured as a matrix and checked in so it can be re-run.
//!
//! **What this file is.** A malformed `ip()` pattern is rejected by the
//! reference in exactly one shape — when the malformed filter IS the
//! entire `| <label filter>` pipeline stage — and silently deferred
//! everywhere else. That rule was read off the reference's structure
//! (`log.NewIPLabelFilter` records the parse error and leaves the matcher
//! nil, `pkg/logql/log/ip.go:94-103 @ v3.7.4`; `PatternError()`'s only
//! caller is `LabelFilterExpr.Stage()`, `pkg/logql/syntax/ast.go:801-809
//! @ v3.7.4`, which type-switches on the stage's WHOLE filterer) and then
//! measured. This is the measurement, as a fixture: every position an
//! `ip()` label filter can occupy × both operators × well-formed and
//! malformed patterns, with the reference's verdict for each.
//!
//! **Why it is checked in.** The claim it backs — "PulsusDB's rejection
//! surface agrees with the reference's, point for point" — was first made
//! from a matrix that existed only in a scratch directory, and its first
//! version measured the wrong layer (it asked `plan()`, which does not
//! compile a pipeline and therefore accepts every malformed pattern). A
//! number nobody else can re-run is not evidence. Here the fixture, the
//! layer and the reference's verdicts are all in the tree:
//!
//! - [`pulsus_agrees_with_the_captured_reference_verdicts`] — hermetic,
//!   the whole matrix, PulsusDB's verdict at the layer a user meets it
//!   (parse → plan → compile, the same compile `exec` runs before any
//!   I/O: `exec.rs:612`, `:906`, `:2290`, `:2576`, propagated with `?`
//!   and surfaced as `logs_api/error.rs`'s 400).
//! - [`the_pre_248_eager_rule_disagrees_with_the_reference_wherever_the_filter_defers`]
//!   — hermetic, the discrimination check: the rule PulsusDB used before
//!   this issue (validate the pattern at every `ip()` label filter) is
//!   replayed over the same matrix and its every disagreement is
//!   enumerated. The fixture is not something any implementation passes.
//! - [`live_matrix_against_the_reference`] — gated on
//!   `PULSUSDB_LOGQL_DIFF_URL`, re-measures all of it against the pinned
//!   container so the committed verdicts cannot silently rot, and checks
//!   the compression this table uses (verdict depends on the position and
//!   on whether the pattern is malformed, never on the operator or on
//!   which malformed pattern) rather than assuming it.

use std::process::Command;

use pulsus_logql::parse;
use pulsus_read::logql::pipeline::CompiledPipeline;
use pulsus_read::logql::plan::Plan;
use pulsus_read::logql::{Direction, PlanCtx, QueryParams, QuerySpec, plan};

/// What a user sees: the query is served, or it is a 400.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    Accept,
    Reject,
}

/// One `ip()` label-filter position. `{F}` is the filter.
///
/// `reference_rejects_malformed` is CAPTURED, not derived: it is what the
/// pinned container answered (see [`live_matrix_against_the_reference`],
/// which re-measures it). It is true exactly where the malformed filter
/// is the whole `| <label filter>` stage of a PIPELINE-stage site —
/// `syntax.y:221 @ v3.7.4` — and false at every nesting, at every
/// post-`unwrap` site (`syntax.y:160`, reduced by `ReduceAndLabelFilter`,
/// `syntax/extractor.go:76,187 @ v3.7.4`, which never calls `Stage()`),
/// and for every well-formed pattern.
struct Position {
    name: &'static str,
    template: &'static str,
    reference_rejects_malformed: bool,
}

const POSITIONS: &[Position] = &[
    // --- pipeline-stage site, the filter alone: the ONLY rejections.
    Position {
        name: "lone",
        template: r#"{service_name="m"} | logfmt | {F}"#,
        reference_rejects_malformed: true,
    },
    Position {
        name: "lone_parens",
        template: r#"{service_name="m"} | logfmt | ({F})"#,
        reference_rejects_malformed: true,
    },
    Position {
        name: "lone_second_stage",
        template: r#"{service_name="m"} | logfmt | b="x" | {F}"#,
        reference_rejects_malformed: true,
    },
    Position {
        name: "lone_before_unwrap",
        template: r#"sum_over_time({service_name="m"} | logfmt | {F} | unwrap val [5m])"#,
        reference_rejects_malformed: true,
    },
    // --- nested in a binary label filter: the filterer is the Binary,
    //     so `PatternError()` is never consulted.
    Position {
        name: "and_lhs",
        template: r#"{service_name="m"} | logfmt | {F} and b="x""#,
        reference_rejects_malformed: false,
    },
    Position {
        name: "and_rhs",
        template: r#"{service_name="m"} | logfmt | b="x" and {F}"#,
        reference_rejects_malformed: false,
    },
    Position {
        name: "comma",
        template: r#"{service_name="m"} | logfmt | {F}, b="x""#,
        reference_rejects_malformed: false,
    },
    Position {
        name: "or_lhs",
        template: r#"{service_name="m"} | logfmt | {F} or b="x""#,
        reference_rejects_malformed: false,
    },
    Position {
        name: "or_rhs",
        template: r#"{service_name="m"} | logfmt | b="x" or {F}"#,
        reference_rejects_malformed: false,
    },
    Position {
        name: "nested_parens",
        template: r#"{service_name="m"} | logfmt | ({F} or b="zzz") or b="x""#,
        reference_rejects_malformed: false,
    },
    Position {
        name: "nested_then_lone_stage",
        template: r#"{service_name="m"} | logfmt | {F} or b="x" | b="x""#,
        reference_rejects_malformed: false,
    },
    // --- post-`unwrap`: EVERY position defers, a lone filter included.
    Position {
        name: "unwrap_lone",
        template: r#"sum_over_time({service_name="m"} | logfmt | unwrap val | {F} [5m])"#,
        reference_rejects_malformed: false,
    },
    Position {
        name: "unwrap_lone_parens",
        template: r#"sum_over_time({service_name="m"} | logfmt | unwrap val | ({F}) [5m])"#,
        reference_rejects_malformed: false,
    },
    Position {
        name: "unwrap_and",
        template: r#"sum_over_time({service_name="m"} | logfmt | unwrap val | {F} and b="x" [5m])"#,
        reference_rejects_malformed: false,
    },
    Position {
        name: "unwrap_second_stage",
        template: r#"sum_over_time({service_name="m"} | logfmt | unwrap val | b="x" | {F} [5m])"#,
        reference_rejects_malformed: false,
    },
    // --- a numeric-family filter to the LEFT (the round-2 shapes): the
    //     conversion can fail, which is an evaluation concern and never a
    //     rejection.
    Position {
        name: "numeric_and",
        template: r#"{service_name="m"} | logfmt | n > 1 and {F}"#,
        reference_rejects_malformed: false,
    },
    Position {
        name: "numeric_or",
        template: r#"{service_name="m"} | logfmt | n > 1 or {F}"#,
        reference_rejects_malformed: false,
    },
    Position {
        name: "numeric_comma",
        template: r#"{service_name="m"} | logfmt | n > 1, {F}"#,
        reference_rejects_malformed: false,
    },
];

/// The two label-filter operators `ip()` accepts.
const OPS: &[&str] = &["=", "!="];

/// `(pattern, malformed)`. The malformed set covers each way
/// `netip`/`ParsePrefix`/the range form can fail; the well-formed set
/// covers CIDR, single address and range.
const PATTERNS: &[(&str, bool)] = &[
    ("10.0.0.0/8", false),
    ("1.2.3.4", false),
    ("1.2.3.4-1.2.3.9", false),
    ("nope", true),
    ("", true),
    ("10.0.0.0/99", true),
    ("10.0.0.0/", true),
    ("1.2.3.4-", true),
    ("999.1.1.1", true),
    ("-1.2.3.4", true),
];

/// One matrix point.
struct Point {
    position: &'static Position,
    op: &'static str,
    pattern: &'static str,
    malformed: bool,
    query: String,
}

fn matrix() -> Vec<Point> {
    let mut out = Vec::new();
    for position in POSITIONS {
        for op in OPS {
            for (pattern, malformed) in PATTERNS {
                let filter = format!(r#"addr {op} ip("{pattern}")"#);
                out.push(Point {
                    position,
                    op,
                    pattern,
                    malformed: *malformed,
                    query: position.template.replace("{F}", &filter),
                });
            }
        }
    }
    out
}

impl Point {
    /// What the pinned reference answers, from the captured table.
    fn reference(&self) -> Verdict {
        if self.malformed && self.position.reference_rejects_malformed {
            Verdict::Reject
        } else {
            Verdict::Accept
        }
    }

    /// What PulsusDB rejected before issue #248: `compile_label_filter`
    /// validated the `ip()` pattern eagerly wherever the filter sat, so
    /// any malformed pattern in a label filter was a compile error. A
    /// MODEL of the old rule, not the old code — its only job is to show
    /// this fixture is not something every implementation satisfies.
    fn pre_248_pulsus(&self) -> Verdict {
        if self.malformed {
            Verdict::Reject
        } else {
            Verdict::Accept
        }
    }

    fn describe(&self) -> String {
        format!(
            "{} {} ip({:?}): {}",
            self.position.name, self.op, self.pattern, self.query
        )
    }
}

fn ctx() -> PlanCtx<'static> {
    PlanCtx {
        db: "pulsus",
        streams_idx: "log_streams_idx",
        streams: "log_streams",
        samples: "log_samples",
        rollup_table: "log_metrics_5s",
        rollup_res_ns: 5_000_000_000,
        scan_budget_bytes: 50 * 1024 * 1024 * 1024,
        max_streams: 100_000,
        pipeline_scan_factor: 10,
    }
}

/// A `query_range` request, because that is the endpoint the live leg
/// puts these to — the reference refuses an instant LOG query outright,
/// which would mask the surface under test.
fn params() -> QueryParams {
    QueryParams {
        spec: QuerySpec::Range {
            start_ns: 1_782_907_200_000_000_000,
            end_ns: 1_782_928_800_000_000_000,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    }
}

/// PulsusDB's verdict **at the layer a user meets it**: parse, then plan,
/// then the pipeline compile `exec` performs before any I/O. Compiling
/// the raw AST instead would be a different layer, and asking `plan()`
/// alone would be a weaker one — it accepts every malformed pattern,
/// which is exactly how the first version of this matrix reported
/// agreement it had not measured.
fn pulsus_verdict(query: &str) -> (Verdict, String) {
    let expr = match parse(query) {
        Ok(e) => e,
        Err(e) => return (Verdict::Reject, format!("parse: {e}")),
    };
    let planned = match plan(&expr, &params(), &ctx()) {
        Ok(p) => p,
        Err(e) => return (Verdict::Reject, format!("plan: {e}")),
    };
    let pipelines = match &planned {
        Plan::Streams(sp) => vec![&sp.pipeline],
        Plan::Metric(mp) => mp.client.iter().map(|c| &c.pipeline).collect(),
        Plan::MetricBinary(_) => panic!(
            "this matrix holds no binary metric query; add the walk before adding one, or the \
             point would be measured as an unconditional accept"
        ),
    };
    for stages in pipelines {
        if let Err(e) = CompiledPipeline::compile(stages) {
            return (Verdict::Reject, format!("compile: {e}"));
        }
    }
    (Verdict::Accept, String::new())
}

/// **The matrix agrees with the reference, point for point.**
#[test]
fn pulsus_agrees_with_the_captured_reference_verdicts() {
    let points = matrix();
    assert_eq!(
        points.len(),
        POSITIONS.len() * OPS.len() * PATTERNS.len(),
        "the matrix must be the full cross product"
    );

    let mut disagree = Vec::new();
    let (mut accepts, mut rejects) = (0usize, 0usize);
    for p in &points {
        let (ours, why) = pulsus_verdict(&p.query);
        match p.reference() {
            Verdict::Accept => accepts += 1,
            Verdict::Reject => rejects += 1,
        }
        if ours != p.reference() {
            disagree.push(format!(
                "{}\n    reference={:?} pulsus={:?} {}",
                p.describe(),
                p.reference(),
                ours,
                why
            ));
        }
    }
    assert!(
        disagree.is_empty(),
        "{} of {} matrix points disagree with the captured reference verdicts:\n{}",
        disagree.len(),
        points.len(),
        disagree.join("\n")
    );
    // Both dispositions must be represented, or "agrees everywhere" would
    // be a statement about a matrix that only ever accepts.
    assert!(
        accepts > 0 && rejects > 0,
        "the matrix must contain both dispositions ({accepts} accept, {rejects} reject)"
    );
}

/// **The fixture discriminates.** The rule PulsusDB shipped before this
/// issue — validate an `ip()` pattern wherever it appears — replayed over
/// the same matrix, with every disagreement enumerated rather than
/// counted.
///
/// Every one of them is the same direction: PulsusDB rejects a query the
/// reference serves. That is the divergence the issue was filed for, and
/// it is what the fixture would report again if the rule regressed.
#[test]
fn the_pre_248_eager_rule_disagrees_with_the_reference_wherever_the_filter_defers() {
    let points = matrix();
    let mut mismatches = Vec::new();
    for p in &points {
        if p.pre_248_pulsus() != p.reference() {
            assert_eq!(
                (p.pre_248_pulsus(), p.reference()),
                (Verdict::Reject, Verdict::Accept),
                "a pre-#248 mismatch in the other direction would be a different bug: {}",
                p.describe()
            );
            mismatches.push(p);
        }
    }
    // Exactly the deferring positions, over every operator and every
    // malformed pattern — computed from the table rather than restated.
    let deferring = POSITIONS
        .iter()
        .filter(|p| !p.reference_rejects_malformed)
        .count();
    let malformed = PATTERNS.iter().filter(|(_, m)| *m).count();
    assert_eq!(
        mismatches.len(),
        deferring * OPS.len() * malformed,
        "the pre-#248 rule must disagree at every deferring position, and nowhere else"
    );
    assert!(
        mismatches
            .iter()
            .all(|p| !p.position.reference_rejects_malformed && p.malformed),
        "a mismatch outside the deferring/malformed set"
    );
    eprintln!(
        "pre-#248 rule: {} of {} points disagree with the reference, all \
         'PulsusDB rejects, reference accepts'",
        mismatches.len(),
        points.len()
    );
}

// ---------------------------------------------------------------------
// The live leg (gated on PULSUSDB_LOGQL_DIFF_URL, like every other leg on
// the differential oracle). Status only: no data is pushed, because
// accept-vs-reject is decided before a single line is read.
// ---------------------------------------------------------------------

/// **The window has to be a servable one, and that is measured, not
/// stylistic.** On the pinned container the same lone malformed filter
/// is a 400 over a recent window and a 200, empty, over `start=0&end=1`
/// or a window ten years back. Putting this matrix to a degenerate
/// window would have scored the reference as accepting all of it, so the
/// window below ends at `now`.
///
/// The boundary is `querier.query_ingesters_within` (3 h by default),
/// bisected by the hour on the pinned container (issue #380): once the
/// query's END is older than that the ingester leaves the query's path
/// (`pkg/querier/intervals.go:32-38 @ v3.7.4`) and only the store is
/// asked, and the store returns `iter.NoopEntryIterator` as soon as no
/// chunk matches (`pkg/storage/store.go:491 @ v3.7.4`) — before
/// `expr.Pipeline()` at `:497`, which is where a malformed `ip()`
/// surfaces. **It is NOT `limitsMiddleware.Do`'s `max_query_lookback`
/// short-circuit** (`pkg/querier/queryrange/limits.go:167-181 @
/// v3.7.4`), which this comment asserted until #380 measured it: the
/// pinned container reports `max_query_lookback: 0s` on `/config`, the
/// shipped default (`pkg/validation/limits.go:379 @ v3.7.4`), so that
/// branch never runs. Ledgered as
/// `malformed-query-refused-in-every-window`.
fn reference_verdict(base: &str, query: &str) -> (Verdict, String) {
    let now_s = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    // `query_range`: v3.7.4 refuses an instant LOG query on type grounds
    // ("log queries are not supported as an instant query type"), which
    // would mask the surface under test behind an unrelated rejection.
    let out = Command::new("curl")
        .args([
            "-s",
            "--max-time",
            "30",
            "-o",
            "/dev/stderr",
            "-w",
            "%{http_code}",
            "-G",
            &format!("{base}/loki/api/v1/query_range"),
            "--data-urlencode",
            &format!("query={query}"),
            "--data-urlencode",
            &format!("start={}", (now_s - 300) * 1_000_000_000),
            "--data-urlencode",
            &format!("end={}", now_s * 1_000_000_000),
            "--data-urlencode",
            "step=60s",
        ])
        .output()
        .expect("curl");
    let code = String::from_utf8_lossy(&out.stdout).trim().to_string();
    let body = String::from_utf8_lossy(&out.stderr).trim().to_string();
    match code.as_str() {
        "200" => (Verdict::Accept, body),
        "400" => (Verdict::Reject, body),
        other => panic!("unexpected status {other} for {query:?}: {body}"),
    }
}

/// **Re-measures the committed verdicts against the pinned container**,
/// and checks the compression they are stored in.
///
/// The table records one bit per position (does a malformed pattern
/// reject here?) for a matrix of position × operator × pattern. That is a
/// claim — that the verdict does not depend on the operator, and does not
/// depend on WHICH malformed pattern — and this leg puts every point
/// individually, so the claim is measured rather than assumed.
#[test]
fn live_matrix_against_the_reference() {
    let Ok(base) = std::env::var("PULSUSDB_LOGQL_DIFF_URL") else {
        eprintln!("PULSUSDB_LOGQL_DIFF_URL unset — skipping the live nested-ip matrix");
        return;
    };
    let points = matrix();
    let mut disagree = Vec::new();
    for p in &points {
        let (theirs, body) = reference_verdict(&base, &p.query);
        if theirs != p.reference() {
            disagree.push(format!(
                "{}\n    committed={:?} container={:?} {}",
                p.describe(),
                p.reference(),
                theirs,
                body.chars().take(160).collect::<String>()
            ));
        }
    }
    assert!(
        disagree.is_empty(),
        "{} of {} committed verdicts no longer match the pinned container — re-capture the \
         table, do not edit it to match one point:\n{}",
        disagree.len(),
        points.len(),
        disagree.join("\n")
    );
    eprintln!(
        "re-measured {} matrix points against the reference; the committed verdicts hold",
        points.len()
    );
}
