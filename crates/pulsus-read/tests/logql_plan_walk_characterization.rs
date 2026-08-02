//! Issue #272 — the plan-side characterization golden (AC 4).
//!
//! Two subjects:
//!
//! * **`MetricNode`** (SCC-3, converted in Wave 1) — `{:?}`, `{:#?}` and
//!   the `PartialEq` cross-product over a fixture matrix, plus a derived
//!   shadow differential proving the hand-written impls are
//!   byte-equivalent to the derive they replaced.
//! * **`CompiledPipeline`** (which reaches `CompiledLabelFilter` through
//!   `CompiledStage::LabelFilter` — the type is private, the bytes are
//!   public). Its `Debug` bytes are captured **now**, before Wave 2
//!   flattens `CompiledLabelFilter` to a `Vec<LfOp>`, so that branch has
//!   a pre-change oracle rather than one written after the fact.
//!
//! The golden is frozen by `characterization_freeze.rs`.

use pulsus_logql::BinOp;
use pulsus_logql::walk::Child;
use pulsus_read::logql::pipeline::CompiledPipeline;
use pulsus_read::logql::template::TemplateEnv;
use pulsus_read::logql::{MetricNode, MetricNodeScc};

// ---------------------------------------------------------------------
// The derived shadow mirror: SCC-3's pre-#272 shape, verbatim.
// ---------------------------------------------------------------------

mod shadow {
    use pulsus_logql::BinOp;
    use pulsus_read::logql::GridWindow;

    #[derive(Debug, Clone, PartialEq)]
    pub enum MetricNode {
        Scalar(f64),
        VectorLit {
            value: f64,
            window: GridWindow,
        },
        Binary {
            op: BinOp,
            return_bool: bool,
            matching: Option<pulsus_logql::VectorMatching>,
            lhs: Box<MetricNode>,
            rhs: Box<MetricNode>,
        },
        VectorAgg {
            aggs: Vec<pulsus_read::logql::plan::VectorAggSpec>,
            inner: Box<MetricNode>,
        },
    }
}

#[derive(Debug, Clone)]
enum Shape {
    Scalar(u32),
    VectorLit,
    VectorAgg(Box<Shape>),
    Binary(BinOp, bool, Box<Shape>, Box<Shape>),
}

fn window() -> pulsus_read::logql::GridWindow {
    pulsus_read::logql::GridWindow {
        start_ns: 1_000,
        end_ns: 2_000,
        step_ns: None,
    }
}

fn build(s: &Shape) -> MetricNode {
    match s {
        Shape::Scalar(i) => MetricNode::Scalar(f64::from(*i)),
        Shape::VectorLit => MetricNode::VectorLit {
            value: 1.5,
            window: window(),
        },
        Shape::VectorAgg(inner) => MetricNode::VectorAgg {
            aggs: Vec::new(),
            inner: Child::new(build(inner)),
        },
        Shape::Binary(op, rb, l, r) => MetricNode::Binary {
            op: *op,
            return_bool: *rb,
            matching: None,
            lhs: Child::new(build(l)),
            rhs: Child::new(build(r)),
        },
    }
}

fn build_shadow(s: &Shape) -> shadow::MetricNode {
    match s {
        Shape::Scalar(i) => shadow::MetricNode::Scalar(f64::from(*i)),
        Shape::VectorLit => shadow::MetricNode::VectorLit {
            value: 1.5,
            window: window(),
        },
        Shape::VectorAgg(inner) => shadow::MetricNode::VectorAgg {
            aggs: Vec::new(),
            inner: Box::new(build_shadow(inner)),
        },
        Shape::Binary(op, rb, l, r) => shadow::MetricNode::Binary {
            op: *op,
            return_bool: *rb,
            matching: None,
            lhs: Box::new(build_shadow(l)),
            rhs: Box::new(build_shadow(r)),
        },
    }
}

fn matrix() -> Vec<(&'static str, Shape)> {
    use Shape::{Binary, Scalar, VectorAgg, VectorLit};
    let b = |op, rb, l: Shape, r: Shape| Binary(op, rb, Box::new(l), Box::new(r));
    vec![
        ("scalar", Scalar(3)),
        ("vector literal", VectorLit),
        ("vector agg over a scalar", VectorAgg(Box::new(Scalar(7)))),
        ("binary plain", b(BinOp::Add, false, Scalar(1), Scalar(2))),
        (
            "binary with the bool modifier",
            b(BinOp::Gt, true, Scalar(1), VectorLit),
        ),
        (
            "left-deep chain",
            b(
                BinOp::Or,
                false,
                b(BinOp::Or, false, Scalar(1), VectorLit),
                VectorAgg(Box::new(Scalar(2))),
            ),
        ),
        (
            "right-deep chain",
            b(
                BinOp::Add,
                false,
                Scalar(1),
                b(BinOp::Sub, false, VectorLit, VectorAgg(Box::new(Scalar(4)))),
            ),
        ),
        (
            "vector agg over a binary",
            VectorAgg(Box::new(b(BinOp::Mul, false, Scalar(5), VectorLit))),
        ),
    ]
}

/// The label-filter matrix whose compiled `Debug` bytes Wave 2 must not
/// move. `CompiledLabelFilter` is private, so the pipeline is the
/// reachable surface.
const LABEL_FILTER_QUERIES: &[&str] = &[
    r#"{a="b"} | x="1""#,
    r#"{a="b"} | x=~"1.*""#,
    r#"{a="b"} | x!="1""#,
    r#"{a="b"} | x!~"1.*""#,
    r#"{a="b"} | logfmt | dur > 1s"#,
    r#"{a="b"} | logfmt | n >= 2"#,
    r#"{a="b"} | logfmt | n < 3 or n > 9"#,
    r#"{a="b"} | logfmt | n = 1 and m = 2"#,
    r#"{a="b"} | logfmt | n = 1, m = 2"#,
    r#"{a="b"} | logfmt | (n = 1 or m = 2) and o = 3"#,
    r#"{a="b"} | logfmt | n = 1 or (m = 2 and o = 3)"#,
    r#"{a="b"} | ip_field = ip("1.2.3.4")"#,
    r#"{a="b"} | ip_field != ip("1.2.3.0/24")"#,
    r#"{a="b"} | line_format "{{.a}}" | x="1""#,
];

/// A fully PINNED template environment.
///
/// `CompiledPipeline::compile` installs the SERVER-CONFIGURED zone
/// (`reader.template_timezone`, default UTC — issue #311). It used to
/// resolve the host's `$TZ` (or `/etc/localtime`) — **ambient input, not
/// an observable of the compiled pipeline**. Freezing that into a golden
/// made the file pass only on a machine whose zone matched whoever last
/// regenerated it: this golden froze `Europe/London` and CI, running
/// `Etc/UTC`, failed on 142 lines. The golden still pins its own env
/// rather than relying on the default, so it is independent of a
/// deployment's configuration too.
///
/// Every field is populated rather than defaulted, so the golden still
/// pins `local`, `local_name` and `now_ns` byte-for-byte and a field
/// moving still reddens it — it just pins a CHOSEN value instead of a
/// discovered one. `with_template_env` is the crate's own mechanism for
/// exactly this (its doc names tests and the hermetic corpus runner),
/// and it is preferred over setting `$TZ` in-process, which is global
/// and unreliable under `--test-threads=2`.
fn pinned_env() -> TemplateEnv {
    TemplateEnv {
        local: Some(chrono_tz::Tz::UTC),
        local_name: Some("Local".to_string()),
        now_ns: Some(1_700_000_000_000_000_000),
    }
}

fn compiled_debug(query: &str) -> (String, String) {
    let expr = pulsus_logql::parse(query).expect("fixture parses");
    let stages = match &expr {
        pulsus_logql::Expr::Log(log) => log.pipeline.clone(),
        other => panic!("unexpected fixture shape: {other:?}"),
    };
    let compiled = CompiledPipeline::compile(&stages)
        .expect("compile")
        .with_template_env(pinned_env());
    (format!("{compiled:?}"), format!("{compiled:#?}"))
}

fn render_golden() -> String {
    let mut out = String::new();
    out.push_str("##### MetricNode #####\n");
    for (name, s) in matrix() {
        let v = build(&s);
        out.push_str("=== ");
        out.push_str(name);
        out.push_str(" ===\n--- Debug {:?} ---\n");
        out.push_str(&format!("{v:?}"));
        out.push_str("\n--- Debug {:#?} ---\n");
        out.push_str(&format!("{v:#?}"));
        out.push('\n');
    }
    out.push_str("\n##### CompiledPipeline (pre-Wave-2) #####\n");
    for q in LABEL_FILTER_QUERIES {
        let (compact, alt) = compiled_debug(q);
        out.push_str("=== ");
        out.push_str(q);
        out.push_str(" ===\n--- Debug {:?} ---\n");
        out.push_str(&compact);
        out.push_str("\n--- Debug {:#?} ---\n");
        out.push_str(&alt);
        out.push('\n');
    }
    out
}

const GOLDEN: &str = include_str!("golden/plan_walk_characterization.txt");

#[test]
fn the_committed_golden_still_describes_the_shipped_types() {
    assert_eq!(
        render_golden(),
        GOLDEN,
        "`MetricNode`'s or `CompiledPipeline`'s observable bytes moved (issue #272 AC 4/11). \
         Wave 2 flattens `CompiledLabelFilter` and must leave this file byte-unchanged."
    );
}

#[test]
fn metric_node_debug_is_byte_identical_to_the_derive() {
    for (name, s) in matrix() {
        let real = build(&s);
        let shadow = build_shadow(&s);
        assert_eq!(
            format!("{real:?}"),
            format!("{shadow:?}"),
            "compact Debug diverged for {name}"
        );
        assert_eq!(
            format!("{real:#?}"),
            format!("{shadow:#?}"),
            "alternate Debug diverged for {name}"
        );
        // Nested inside another derived `{:#?}`, which is where a wrong
        // padding level shows up.
        assert_eq!(
            format!("{:#?}", (1u8, build(&s), "tail")),
            format!("{:#?}", (1u8, build_shadow(&s), "tail")),
            "nested alternate Debug diverged for {name}"
        );
    }
}

#[test]
fn metric_node_partial_eq_agrees_with_the_derive_over_the_cross_product() {
    let m = matrix();
    for (an, a) in &m {
        for (bn, b) in &m {
            let want = build_shadow(a) == build_shadow(b);
            let got = build(a) == build(b);
            assert_eq!(got, want, "PartialEq diverged for {an} vs {bn}");
        }
    }
}

#[test]
fn metric_node_clone_reproduces_the_original() {
    for (name, s) in matrix() {
        let real = build(&s);
        let copy = real.clone();
        assert!(real == copy, "clone diverged for {name}");
        assert_eq!(format!("{real:#?}"), format!("{copy:#?}"));
    }
}

#[test]
fn post_order_is_children_before_parents_left_to_right() {
    fn expect(s: &Shape, out: &mut Vec<&'static str>) {
        match s {
            Shape::Scalar(_) => out.push("Scalar"),
            Shape::VectorLit => out.push("VectorLit"),
            Shape::VectorAgg(inner) => {
                expect(inner, out);
                out.push("VectorAgg");
            }
            Shape::Binary(_, _, l, r) => {
                expect(l, out);
                expect(r, out);
                out.push("Binary");
            }
        }
    }
    for (name, s) in matrix() {
        let tree = build(&s);
        let mut nodes = Vec::new();
        pulsus_logql::walk::postorder_into::<MetricNodeScc>(&tree, &mut nodes);
        let got: Vec<&'static str> = nodes
            .iter()
            .map(|n| match n {
                MetricNode::Leaf(_) => "Leaf",
                MetricNode::Scalar(_) => "Scalar",
                MetricNode::VectorLit { .. } => "VectorLit",
                MetricNode::Binary { .. } => "Binary",
                MetricNode::VectorAgg { .. } => "VectorAgg",
                MetricNode::Variants { .. } => "Variants",
            })
            .collect();
        let mut want = Vec::new();
        expect(&s, &mut want);
        assert_eq!(got, want, "post-order sequence for {name}");
    }
}

/// `cargo test -p pulsus-read --test logql_plan_walk_characterization -- --ignored zz_regenerate`
#[test]
#[ignore = "generator: rewrites the committed golden"]
fn zz_regenerate_golden() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden");
    std::fs::create_dir_all(&dir).expect("golden dir");
    let body = render_golden();
    std::fs::write(dir.join("plan_walk_characterization.txt"), &body).expect("write golden");
    let digest = <sha2::Sha256 as sha2::Digest>::digest(body.as_bytes());
    std::fs::write(
        dir.join("plan_walk_characterization.sha256"),
        format!("{digest:x}\n"),
    )
    .expect("write sha256");
}
