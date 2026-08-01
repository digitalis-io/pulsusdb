//! Issue #260 — the template render budget COMPOSES across the renders
//! one row performs.
//!
//! `MAX_TEMPLATE_RENDER_BYTES` used to be enforced by a budget
//! constructed inside `render_full`, whose output the caller then
//! RETAINS: `line_format` moves it into the line, and every
//! `label_format` destination `set_label`s it. So the cap bounded ONE
//! live buffer while the number of simultaneously-live buffers was
//! bounded only by the query-text cap — a `label_format` stage's
//! destination count. A sum over an unbounded multiplicity is not a
//! bound, so the budget's lifetime moved from the render to the ROW.
//!
//! These are the two halves of that statement: the multiplicity the
//! query-text cap really admits (thousands, not one), and the refusal
//! that now happens at the budget instead of at the allocator.
//!
//! **The `Simple`/`Parts` fast paths are covered here too** (review
//! finding 1). They never call `render_full` — they copy a resolved
//! value or a presized part list straight into the destination — and
//! `label_format d="{{.a}}"` compiles to `Simple`, so a gate built only
//! from `Full`/`repeat` fixtures would pass while the shape most queries
//! actually use stayed unbounded. Every fixture below exists in both
//! flavours.

use std::borrow::Cow;

use pulsus_read::logql::CompiledPipeline;
use pulsus_read::logql::template::{
    self, MAX_TEMPLATE_RENDER_BYTES, RenderBudget, Template, TemplateEnv, TemplateKind,
};

fn base() -> Vec<(String, String)> {
    vec![("a".to_string(), "x".to_string())]
}

fn compiled(query: &str) -> CompiledPipeline {
    let expr = pulsus_logql::parse(query).expect("parse");
    let pulsus_logql::Expr::Log(log) = expr else {
        panic!("expected a log query");
    };
    CompiledPipeline::compile(&log.pipeline)
        .expect("compile")
        .with_template_env(TemplateEnv::default())
}

/// The LogQL source of one `label_format` destination that renders
/// `repeat_count` bytes: `d<i>="{{repeat <n> \"x\"}}"`.
fn destination(index: usize, repeat_count: usize) -> String {
    format!(r#"d{index}="{{{{repeat {repeat_count} \"x\"}}}}""#)
}

/// The template BODY of [`destination`], with the LogQL string escaping
/// undone — what `label_format` compiles and renders.
fn destination_body(repeat_count: usize) -> String {
    format!(r#"{{{{repeat {repeat_count} "x"}}}}"#)
}

fn label_format_query(count: usize, repeat_count: usize) -> String {
    let mut q = String::from(r#"{app="x"} | label_format "#);
    for i in 0..count {
        if i > 0 {
            q.push(',');
        }
        q.push_str(&destination(i, repeat_count));
    }
    q
}

/// **The multiplicity the query-text cap admits** — thousands of
/// destinations, each of which used to get its own full budget.
///
/// The count is DERIVED from `pulsus_logql::MAX_QUERY_BYTES` rather than
/// written down, and the query it produces is parsed to prove the shape
/// is really admissible (an unreachable shape would make the whole
/// finding theoretical).
#[test]
fn the_query_text_cap_admits_thousands_of_label_format_destinations() {
    let cap = pulsus_logql::MAX_QUERY_BYTES;
    // Grow the destination list until one more would cross the cap.
    let mut count = 0usize;
    loop {
        let next = label_format_query(count + 1, 1 << 20);
        if next.len() >= cap {
            break;
        }
        count += 1;
    }
    let query = label_format_query(count, 1 << 20);
    assert!(query.len() < cap);
    pulsus_logql::parse(&query).expect("the maximal destination list must parse");

    assert!(
        count >= 1_000,
        "the query-text cap admits only {count} destinations — the unbounded-plurality \
         finding rests on this being in the thousands"
    );
    // Per-render budgets would have made this many 64 MiB outputs
    // simultaneously live. Stated as the arithmetic, not as a claim.
    let unbounded = count as u64 * MAX_TEMPLATE_RENDER_BYTES;
    assert!(
        unbounded > 250 * 1024 * 1024 * 1024,
        "a per-render budget bounded {unbounded} bytes of simultaneously-live output"
    );
}

/// **The refusal that used to be an allocation.** A `label_format` stage
/// whose destinations each fit the budget comfortably, but whose SUM does
/// not, is now the bounded 422 — at the budget, before the allocator.
///
/// The fixture is deliberately in the shape the finding describes: many
/// destinations, each individually small enough that a per-render budget
/// would serve every one of them (proved by
/// `each_destination_alone_fits_the_budget_under_a_per_render_lifetime`).
#[test]
fn a_label_format_stages_destinations_share_one_row_budget() {
    // One render charges roughly twice its output (the value is charged
    // when built and again when printed), so ~32 destinations of 1 MiB
    // reach the budget; the extra ones make the fixture unambiguous.
    const PER_DESTINATION: usize = 1 << 20;
    let count = (MAX_TEMPLATE_RENDER_BYTES / PER_DESTINATION as u64) as usize + 4;

    let pipeline = compiled(&label_format_query(count, PER_DESTINATION));
    // `Ok(_)`, never `expect_err`: the served value is tens of MiB of
    // rendered labels, and a failure that prints it is unreadable.
    match pipeline.run("line", &base(), 0) {
        Err(e) => assert_eq!(e.budget_bytes, MAX_TEMPLATE_RENDER_BYTES),
        Ok(_) => panic!(
            "{count} destinations of {PER_DESTINATION} bytes were served — the row's renders \
             are not sharing one budget"
        ),
    }

    // The metric path shares the identical lifetime.
    let mut labels: Vec<(Cow<'_, str>, Cow<'_, str>)> = Vec::new();
    let base = base();
    if pipeline
        .run_metric_into("line", &base, 0, &mut labels)
        .is_ok()
    {
        panic!("the metric path must compose identically");
    }
}

/// The other half of the previous test, and the reason it is a
/// REGRESSION gate rather than a tautology: under the OLD per-render
/// lifetime — reconstructed here by handing `render_full` a fresh budget
/// per destination — the very same fixture is accepted, every
/// destination rendering its full output. Only the shared lifetime
/// refuses it.
#[test]
fn each_destination_alone_fits_the_budget_under_a_per_render_lifetime() {
    const PER_DESTINATION: usize = 1 << 20;
    let count = (MAX_TEMPLATE_RENDER_BYTES / PER_DESTINATION as u64) as usize + 4;

    let Template::Full(prog) =
        template::compile(&destination_body(PER_DESTINATION), TemplateKind::Label)
            .expect("compile")
    else {
        panic!("the amplifying destination must need the full engine");
    };
    let labels: Vec<(Cow<'_, str>, Cow<'_, str>)> = vec![(Cow::Borrowed("a"), Cow::Borrowed("x"))];
    let env = TemplateEnv::default();

    // A budget per render — the pre-#260 lifetime. Every destination is
    // served, so the plurality was genuinely unbounded.
    for i in 0..count {
        let per_render = RenderBudget::default();
        let rendered =
            template::render_full(&prog, &labels, None, None, "line", 0, &env, &per_render)
                .unwrap_or_else(|e| panic!("destination {i} must fit its own budget: {}", e.msg));
        assert_eq!(rendered.as_str().len(), PER_DESTINATION);
    }

    // One budget for all of them — the shipped lifetime. It refuses, and
    // it refuses on the budget, not on any single render's size.
    let shared = RenderBudget::default();
    let mut breached_at = None;
    for i in 0..count {
        if let Err(e) = template::render_full(&prog, &labels, None, None, "line", 0, &env, &shared)
        {
            assert!(
                e.budget_breach,
                "the refusal must be the budget's: {}",
                e.msg
            );
            breached_at = Some(i);
            break;
        }
    }
    let breached_at = breached_at.expect("the shared budget must refuse before the last render");
    assert!(
        breached_at > 0 && breached_at < count,
        "the breach must land mid-stage (at destination {breached_at} of {count}), not on the \
         first render — a single destination fits"
    );
    assert!(shared.breached());
}

/// A `line_format` followed by `label_format` shares the row's budget
/// too — the two call sites are one lifetime, not two.
#[test]
fn line_format_and_label_format_charge_the_same_row_budget() {
    let half = MAX_TEMPLATE_RENDER_BYTES / 2;
    // Each stage alone renders exactly at the boundary (the cumulative
    // build+print charge makes budget/2 the single-render maximum), so
    // either one on its own is served and the PAIR cannot be.
    let solo_line = compiled(&format!(
        r#"{{app="x"}} | line_format "{{{{repeat {half} \"x\"}}}}""#
    ));
    solo_line
        .run("line", &base(), 0)
        .expect("a lone maximal line_format is served")
        .expect("kept");

    let both = compiled(&format!(
        r#"{{app="x"}} | line_format "{{{{repeat {half} \"x\"}}}}" | label_format d="{{{{repeat {half} \"y\"}}}}""#
    ));
    match both.run("line", &base(), 0) {
        Err(e) => assert_eq!(e.budget_bytes, MAX_TEMPLATE_RENDER_BYTES),
        Ok(_) => panic!(
            "a maximal line_format AND a maximal label_format in one row were served — the \
             two render sites are not sharing one budget"
        ),
    }
}

/// The budget is per ROW, not per QUERY: a modest template that fits is
/// served for row after row, with each row starting from a full budget.
/// Without the reset, a long-running streams query would 422 on line N
/// for no reason a client could act on.
#[test]
fn every_row_starts_from_a_full_budget() {
    let eighth = MAX_TEMPLATE_RENDER_BYTES / 8;
    let pipeline = compiled(&format!(
        r#"{{app="x"}} | line_format "{{{{repeat {eighth} \"x\"}}}}""#
    ));
    let base = base();
    for row in 0..6 {
        let out = pipeline
            .run("line", &base, row)
            .unwrap_or_else(|e| panic!("row {row} must be served: {e}"))
            .expect("kept");
        assert_eq!(out.line.len() as u64, eighth);
    }
}

// ---------------------------------------------------------------------
// The `Simple` / `Parts` fast paths (review finding 1). These never
// reach `render_full`, so nothing above exercises them — and they are
// the shape `label_format d="{{.a}}"` compiles to.
// ---------------------------------------------------------------------

/// One MiB of admitted label value, the thing the fast paths copy.
const BIG: usize = 1 << 20;

fn base_with_big() -> Vec<(String, String)> {
    vec![
        ("a".to_string(), "x".to_string()),
        ("big".to_string(), "b".repeat(BIG)),
    ]
}

/// Enough copies of `BIG` to cross the budget, plus slack.
fn fast_path_destinations() -> usize {
    (MAX_TEMPLATE_RENDER_BYTES / BIG as u64) as usize + 4
}

fn fast_path_label_format(count: usize, body: &str) -> String {
    let mut q = String::from(r#"{app="x"} | label_format "#);
    for i in 0..count {
        if i > 0 {
            q.push(',');
        }
        q.push_str(&format!(r#"d{i}="{body}""#));
    }
    q
}

/// The compiled shape is what the test claims it is. A fixture that
/// silently derived `Full` would prove nothing about the fast paths, and
/// the derivation rules are not this test's to assume.
#[test]
fn the_fast_path_fixtures_really_compile_to_simple_and_parts() {
    assert!(
        matches!(
            template::compile("{{.big}}", TemplateKind::Label).expect("compile"),
            Template::Simple(ref n) if n == "big"
        ),
        "`{{{{.big}}}}` must derive the Simple fast path"
    );
    assert!(
        matches!(
            template::compile("{{.big}}!", TemplateKind::Label).expect("compile"),
            Template::Parts(_)
        ),
        "`{{{{.big}}}}!` must derive the Parts fast path"
    );
}

/// **The finding.** `label_format` destinations that compile to `Simple`
/// each `set_label` their own copy of the resolved value. Thousands of
/// retained copies of one admitted label used to cost nothing against the
/// budget; they now share the row's ledger like every other render.
///
/// One destination is served, so the refusal is COMPOSITIONAL — it is
/// not a size limit on the value.
#[test]
fn simple_label_format_destinations_share_the_row_budget() {
    let base = base_with_big();
    let count = fast_path_destinations();

    let one = compiled(&fast_path_label_format(1, "{{.big}}"));
    let out = one
        .run("line", &base, 0)
        .expect("a single Simple destination is well inside the budget")
        .expect("kept");
    assert_eq!(
        out.labels
            .iter()
            .find(|(k, _)| k == "d0")
            .expect("d0")
            .1
            .len(),
        BIG
    );
    drop(out);

    let many = compiled(&fast_path_label_format(count, "{{.big}}"));
    match many.run("line", &base, 0) {
        Err(e) => assert_eq!(e.budget_bytes, MAX_TEMPLATE_RENDER_BYTES),
        Ok(_) => panic!(
            "{count} Simple destinations retained {BIG} bytes each without charging the row \
             budget — the fast path bypasses it"
        ),
    }
}

/// The same for `Parts` (text + field), whose exact presize is charged
/// inside the one place it allocates.
#[test]
fn parts_label_format_destinations_share_the_row_budget() {
    let base = base_with_big();
    let count = fast_path_destinations();

    let one = compiled(&fast_path_label_format(1, "{{.big}}!"));
    one.run("line", &base, 0)
        .expect("a single Parts destination is well inside the budget")
        .expect("kept");

    let many = compiled(&fast_path_label_format(count, "{{.big}}!"));
    match many.run("line", &base, 0) {
        Err(e) => assert_eq!(e.budget_bytes, MAX_TEMPLATE_RENDER_BYTES),
        Ok(_) => panic!(
            "{count} Parts destinations retained {BIG}+1 bytes each without charging the row \
             budget"
        ),
    }
}

/// A chain of fast-path `line_format` stages: each one RETAINS its
/// rewritten line, so the copies compose exactly as the destinations do.
#[test]
fn chained_fast_path_line_formats_share_the_row_budget() {
    let base = base_with_big();
    let count = fast_path_destinations();

    let mut q = String::from(r#"{app="x"}"#);
    for _ in 0..count {
        q.push_str(r#" | line_format "{{.big}}""#);
    }
    let pipeline = compiled(&q);
    match pipeline.run("line", &base, 0) {
        Err(e) => assert_eq!(e.budget_bytes, MAX_TEMPLATE_RENDER_BYTES),
        Ok(_) => panic!(
            "{count} chained Simple line_format rewrites were served without charging the row \
             budget"
        ),
    }

    // A short chain over ordinary lines is untouched — the gate must not
    // start rejecting the shape the corpus is full of.
    let short = compiled(
        r#"{app="x"} | line_format "{{.a}}" | line_format "{{.a}}" | line_format "{{.a}}""#,
    );
    let out = short
        .run("line", &base, 0)
        .expect("ordinary chained line_format must stay served")
        .expect("kept");
    assert_eq!(out.line, "x");
}

/// The metric path charges the fast paths identically — `run_metric_into`
/// and `run_into` share `run_mode_into`, and the review's concern was a
/// path that skips the ledger, not a path that skips a caller.
#[test]
fn the_metric_path_charges_the_fast_paths_too() {
    let base = base_with_big();
    let count = fast_path_destinations();
    let pipeline = compiled(&fast_path_label_format(count, "{{.big}}"));
    let mut labels: Vec<(Cow<'_, str>, Cow<'_, str>)> = Vec::new();
    if pipeline
        .run_metric_into("line", &base, 0, &mut labels)
        .is_ok()
    {
        panic!("the metric path must charge the Simple fast path too");
    }
}

// ---------------------------------------------------------------------
// The once-per-stage label SNAPSHOT (review round 2, finding 1) — the
// third retention point, and the one that motivated replacing
// site-by-site charging with a charging TYPE.
// ---------------------------------------------------------------------

/// A size that fits TWICE inside the budget but not three times, so the
/// fixture below distinguishes "the snapshot is charged" from "it is
/// not" by exactly one term. Derived, not chosen.
fn third_of_budget() -> usize {
    (MAX_TEMPLATE_RENDER_BYTES / 3) as usize + (1 << 20)
}

/// **A `label_format` stage that needs a snapshot deep-copies every
/// OWNED value in the label set** — including template output the row
/// has already been charged for — so a stage sequence can put three
/// copies live while charging for two.
///
/// The reference builds the data map once per stage (`fmt.go:423-425`),
/// which is why the copy exists: destinations render against the labels
/// as they were at stage entry while the live vector keeps changing.
///
/// Sized so that stage 1's output plus the snapshot's copy of it plus
/// stage 2's own output is over budget, while any two of the three are
/// under it. That is the reviewer's 96-MiB-on-a-64-MiB-ledger shape,
/// scaled to the constant: uncharged, this is served.
#[test]
fn a_snapshot_requiring_stage_charges_the_labels_it_deep_copies() {
    let n = third_of_budget();
    assert!(2 * n as u64 <= MAX_TEMPLATE_RENDER_BYTES);
    assert!(3 * n as u64 > MAX_TEMPLATE_RENDER_BYTES);
    let base = vec![
        ("app".to_string(), "x".to_string()),
        ("big".to_string(), "b".repeat(n)),
    ];

    // `e1` reads `e0`, which the same stage just wrote — the #231 rule
    // that makes the stage snapshot.
    let query =
        r#"{app="x"} | label_format d0="{{.big}}" | label_format e0="{{.big}}",e1="{{.e0}}""#;

    // That the stage really SNAPSHOTS is observed, not assumed: run the
    // identical query over a tiny label set and `e1` comes back empty,
    // because it read `e0` from the stage-entry copy where `e0` did not
    // yet exist. Without a snapshot it would read the live vector and
    // see the value `e0` had just been given.
    let tiny = vec![
        ("app".to_string(), "x".to_string()),
        ("big".to_string(), "small".to_string()),
    ];
    let pipeline = compiled(query);
    let out = pipeline
        .run("line", &tiny, 0)
        .expect("the tiny fixture is served")
        .expect("kept");
    assert_eq!(
        out.labels.iter().find(|(k, _)| k == "e1").expect("e1").1,
        "",
        "the fixture must be a snapshot-requiring stage, or it deep-copies nothing"
    );
    drop(out);

    // Two of the three copies fit: stage 1 alone is served.
    let one_stage = compiled(r#"{app="x"} | label_format d0="{{.big}}""#);
    one_stage
        .run("line", &base, 0)
        .expect("one retained copy is well inside the budget")
        .expect("kept");

    drop(pipeline);
    let pipeline = compiled(query);
    match pipeline.run("line", &base, 0) {
        Err(e) => assert_eq!(e.budget_bytes, MAX_TEMPLATE_RENDER_BYTES),
        Ok(_) => panic!(
            "a retained {n}-byte label, its uncharged snapshot copy and another {n}-byte \
             render were all served — the stage snapshot is not charging the row budget"
        ),
    }
}

/// The snapshot charges what it DEEP-copies and not what it re-points
/// at: a label set of borrowed base labels costs almost nothing, so an
/// ordinary snapshot-requiring stage over ordinary labels is untouched.
/// Without this the previous test would pass under a charge that simply
/// over-counted everything.
#[test]
fn a_snapshot_over_borrowed_labels_costs_almost_nothing() {
    // Every value here is a BASE label — borrowed by the pipeline, so
    // its clone is a pointer copy.
    let base = vec![
        ("app".to_string(), "x".to_string()),
        ("big".to_string(), "b".repeat(third_of_budget())),
    ];
    let query = r#"{app="x"} | label_format e0="{{.app}}",e1="{{.e0}}""#;
    let pipeline = compiled(query);
    let out = pipeline
        .run("line", &base, 0)
        .expect("a borrowed-label snapshot must stay served")
        .expect("kept");
    assert_eq!(
        out.labels.iter().find(|(k, _)| k == "e1").expect("e1").1,
        "",
        "`e1` reads `e0` from the STAGE SNAPSHOT, where it did not yet exist"
    );
}
