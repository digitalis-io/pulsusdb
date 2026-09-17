//! **Every statement this engine renders for the thirty queries below.**
//!
//! # What this freeze is, and what it is not
//!
//! **It is an external audit anchor, not a self-check.** A reader
//! re-derives its digest by running [`render`] in a worktree of their own
//! and comparing. That protection is exercised by a person; it is worth
//! exactly as much as that comparison.
//!
//! What it is NOT: a check that a coordinated edit of the generator, the
//! golden and the digest is impossible. It is not. Measured at issue
//! #548's merge base, three probes on the generator:
//!
//! ```text
//!   generator edit                      digest      lines   bytes    entries
//!   STEP_MS   60,000 -> 30,000          87c1641f     510    26,727     30
//!   START_MS  moved one minute later    c278302d     510    26,727     30
//!   one query swapped for another       2568fffd     523    27,506     30
//!   one query dropped (time())          a1cb6b7e     506    26,636     29
//! ```
//!
//! [`the_freeze_has_the_published_entry_line_and_byte_counts`] catches
//! the last two — an edit that changes the entry count or the rendered
//! size. Nothing mechanical catches the first two, and the four constants
//! it asserts are published in the issue's implementation notes so that
//! silencing it means editing four numbers a reader can check.
//!
//! **And it is a characterization of the BUILDERS, not of what the server
//! sends.** [`render`] calls `pulsus_promql::plan`,
//! `metrics::grouped::shape_of` and the statement builders directly.
//! Measured at issue #548's base: inserting
//! `let lower_excl = lower_excl + 1;` after `sel.fetch_window(..)` in
//! `crates/pulsus-read/src/metrics/exec.rs` shifts every fetch window the
//! engine sends and leaves this digest unmoved with all of
//! `pulsus-read --lib` green. That hole is what
//! `tests/live_metrics_plan_parts.rs` exists for: it reads the statements
//! back out of `system.query_log`.
//!
//! # Issue #549 moved it, and which four entries moved
//!
//! `max by (status) (…)`, `min without (instance) (…)`, `count(…)` and
//! `group(…)` over a plain instant selector now compile into ONE
//! statement over both sample tables instead of a float fetch and a
//! complementary histogram fetch. So the corpus went from 60 statements
//! to 56 across the same 30 entries, and every other entry is
//! byte-identical.
//!
//! # Every boundary in this golden is writer-emitted
//!
//! The writer emits `-- statement[i]` before each statement, and the live
//! corpus check (`tests/live_sql_corpus_ast.rs`) splits on those markers.
//! No rule infers a boundary from a keyword: the four grouped statements
//! BEGIN with the binding keyword, so a "a line starting `SELECT ` in
//! column 0" rule would find their inner `SELECT` and feed a parser only
//! the tail — under which a prohibited relational binding becomes a clean
//! tree. The marker prefix is reserved: [`render`] refuses to emit a
//! statement containing it.

use sha2::{Digest, Sha256};

use pulsus_promql::{DEFAULT_LOOKBACK_MS, PlanParams, parse, plan};
use pulsus_read::metrics::grouped::Grid;
use pulsus_read::metrics::{MetricsConfig, grouped, grouped_sql, sample_sql};

/// The marker the writer emits before every statement, and the prefix the
/// live corpus check splits on (issue #549).
///
/// **RESERVED.** [`render`] refuses to emit a statement whose text
/// contains it, so a comment of that shape inside a statement cannot
/// manufacture an extra part for the splitter to find.
const STATEMENT_MARKER: &str = "-- statement[";

/// One marker line, rendered:
///
/// ```text
/// -- statement[003] offset=00001274 len=000412
/// ```
///
/// **Every field is fixed width**, so the line's own length does not
/// depend on the numbers in it — which is what lets the writer compute
/// the offset of the statement that follows in ONE pass: it is the
/// output length plus [`MARKER_LEN`].
///
/// The offset and length are what the corpus check splits on. No rule
/// downstream infers a boundary from the text, because the four grouped
/// statements begin with the binding keyword and a keyword rule would
/// find their inner `SELECT` and hand a parser only the tail.
const MARKER_LEN: usize = "-- statement[000] offset=00000000 len=000000\n".len();

fn marker_line(index: usize, offset: usize, len: usize) -> String {
    let line = format!("{STATEMENT_MARKER}{index:03}] offset={offset:08} len={len:06}\n");
    assert_eq!(line.len(), MARKER_LEN, "the marker line is fixed width");
    line
}

/// The group id of each of [`FPS`], **stated here rather than resolved**:
/// the grouped read assigns group ids in our own process from the label
/// sets the resolver returned, and this freeze has no resolver. Writing
/// them out keeps the golden from moving with a fixture it does not have.
const GIDS: [u32; 3] = [0, 1, 0];

/// The engine configuration this freeze renders against: the grouped
/// push ON, because the freeze is a characterization of what the
/// statement builders produce and the pushed statement is one of them.
fn freeze_config() -> MetricsConfig {
    MetricsConfig {
        db: "d".to_string(),
        samples_table: SAMPLES.to_string(),
        hist_samples_table: HIST.to_string(),
        series_table: "metric_series".to_string(),
        metadata_table: "metric_metadata".to_string(),
        experimental_functions: true,
        max_metric_fanout: 1_000,
        max_cache_scan: 200_000,
        max_info_series: 100_000,
        max_samples: 50_000_000,
        distributed: false,
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        grouped_push: true,
    }
}

const GOLDEN: &str = include_str!("golden/promql_statements.txt");
const PINNED: &str = include_str!("golden/promql_statements.sha256");

/// The three constants published on issue #548 before the code existed.
const ENTRIES: usize = 30;
const LINES: usize = 736;
const BYTES: usize = 36_859;
/// The statements the writer's markers declare. Sixty before issue #549;
/// four entries now send ONE statement where they sent two.
const STATEMENTS: usize = 56;

const START_MS: i64 = 1_782_907_200_000;
const END_MS: i64 = 1_782_928_800_000;
const STEP_MS: i64 = 60_000;
const FPS: [u64; 3] = [101, 205, 990];
const SAMPLES: &str = "metric_samples";
const HIST: &str = "metric_hist_samples";

fn names() -> Vec<String> {
    vec![
        "http_requests_total".to_string(),
        "http_errors_total".to_string(),
    ]
}

/// (query, instant?)
#[rustfmt::skip]
const QUERIES: &[(&str, bool)] = &[
    ("http_requests_total{status=\"500\"}", false),
    ("abs(http_requests_total{status=\"500\"})", false),
    ("max by (status) (http_requests_total{status=\"500\"})", false),
    ("min without (instance) (http_requests_total{status=\"500\"})", false),
    ("count(http_requests_total{status=\"500\"})", false),
    ("group(http_requests_total{status=\"500\"})", false),
    ("topk(3, http_requests_total{status=\"500\"})", false),
    ("count_values(\"v\", http_requests_total{status=\"500\"})", false),
    ("stddev by (status) (http_requests_total{status=\"500\"})", false),
    ("histogram_quantile(0.5, http_requests_total{status=\"500\"})", false),
    ("label_replace(http_requests_total{status=\"500\"}, \"l\", \"$1\", \"status\", \"(.*)\")", false),
    ("timestamp(http_requests_total{status=\"500\"})", false),
    ("absent(http_requests_total{status=\"500\"})", false),
    ("sort_desc(http_requests_total{status=\"500\"})", true),
    ("rate(http_requests_total{status=\"500\"}[5m])", false),
    ("sum by (status) (rate(http_requests_total{status=\"500\"}[5m]))", false),
    ("max_over_time(http_requests_total{status=\"500\"}[1m])", false),
    ("quantile_over_time(0.5, http_requests_total{status=\"500\"}[10m])", false),
    ("avg_over_time(http_requests_total{status=\"500\"}[5m] offset 1h)", false),
    ("sum_over_time(http_requests_total{status=\"500\"}[5m] @ 1782900000)", false),
    ("rate(http_requests_total{status=\"500\"}[5m:1m])", false),
    ("http_requests_total{status=\"500\"}[5m]", true),
    ("time()", false),
    ("vector(1)", false),
    ("pi()", false),
    ("info(http_requests_total{status=\"500\"})", false),
    ("rate(http_requests_total{status=\"500\"}[5m]) / on (instance) rate(http_errors_total{status=\"500\"}[5m])", false),
    ("http_requests_total{status=\"500\"} and http_requests_total{status=\"200\"}", false),
    ("http_requests_total{status=\"500\"} * 100", false),
    ("{__name__=~\"http_.*\",status=\"500\"}", false),
];

fn render() -> String {
    let mut out = String::new();
    let mut statement = 0usize;
    // Emits one statement, preceded by its marker. Every boundary in this
    // golden is WRITER-EMITTED: no rule downstream infers one from a
    // keyword, because the grouped statement begins with `WITH` and a
    // keyword rule would find its inner `SELECT` and feed a parser only
    // the tail.
    let mut emit = |out: &mut String, sql: &str| {
        assert!(
            !sql.contains(STATEMENT_MARKER),
            "a statement contains the reserved marker prefix {STATEMENT_MARKER:?}"
        );
        let offset = out.len() + MARKER_LEN;
        out.push_str(&marker_line(statement, offset, sql.len()));
        assert_eq!(
            out.len(),
            offset,
            "the declared offset is where the bytes are"
        );
        out.push_str(sql);
        out.push('\n');
        statement += 1;
    };
    for (q, instant) in QUERIES {
        let params = PlanParams {
            start_ms: START_MS,
            end_ms: if *instant { START_MS } else { END_MS },
            step_ms: if *instant { 0 } else { STEP_MS },
            lookback_ms: DEFAULT_LOOKBACK_MS,
            experimental_functions: true,
        };
        out.push_str(&format!("== {q} ==\n"));
        out.push_str(&format!(
            "params start_ms={} end_ms={} step_ms={}\n",
            params.start_ms, params.end_ms, params.step_ms
        ));
        let p = plan(&parse(q).expect("parse"), params).expect("plan");
        out.push_str(&format!("selectors {}\n", p.selectors.len()));
        // Issue #549: the same decision `exec` takes, so an eligibility
        // change reaches this golden.
        let pushed = grouped::shape_of(&p, &params, &freeze_config());
        for (i, sel) in p.selectors.iter().enumerate() {
            let (lo, hi) = sel.fetch_window(&params);
            out.push_str(&format!(
                "-- selector[{i}] name={} range_ms={} window=({lo}, {hi}]\n",
                sel.metric_name.as_deref().unwrap_or("<none>"),
                sel.range_ms
                    .map_or_else(|| "-".to_string(), |r| r.to_string()),
            ));
            match (&pushed, &sel.metric_name) {
                // ONE statement, over both tables, with the group ids
                // stated above.
                (Some(shape), Some(n)) if shape.selector == sel.id => {
                    out.push_str(&format!("-- grouped op={:?} gids={GIDS:?}\n", shape.op));
                    emit(
                        &mut out,
                        &grouped_sql::grouped_fetch(
                            SAMPLES, HIST, n, &FPS, &GIDS, shape.grid, lo, hi, shape.op,
                        ),
                    );
                }
                (_, Some(n)) => {
                    emit(
                        &mut out,
                        &sample_sql::sample_fetch(SAMPLES, n, &FPS, lo, hi),
                    );
                    emit(
                        &mut out,
                        &sample_sql::hist_sample_fetch(HIST, n, &FPS, lo, hi),
                    );
                }
                (_, None) => {
                    emit(
                        &mut out,
                        &sample_sql::sample_fetch_multi(SAMPLES, &names(), &FPS, lo, hi),
                    );
                    emit(
                        &mut out,
                        &sample_sql::hist_sample_fetch_multi(HIST, &names(), &FPS, lo, hi),
                    );
                }
            }
        }
        out.push('\n');
    }
    out
}

/// The statement count the markers declare — the tripwire, not the
/// protection (issue #549). The protection is the live corpus check,
/// which asserts byte coverage first and then parses.
#[test]
fn the_freeze_declares_its_statement_count() {
    assert_eq!(
        GOLDEN.matches(STATEMENT_MARKER).count(),
        STATEMENTS,
        "the golden's `{STATEMENT_MARKER}` marker count"
    );
    // The markers are numbered 0..STATEMENTS, in order, with none
    // missing — so a splitter that walks them cannot silently skip one —
    // and each one's declared span holds the bytes it says it does.
    for i in 0..STATEMENTS {
        let at = GOLDEN
            .find(&format!("{STATEMENT_MARKER}{i:03}]"))
            .unwrap_or_else(|| panic!("marker {i} is missing"));
        let line = &GOLDEN[at..at + MARKER_LEN];
        let offset: usize = line[line.find("offset=").expect("offset") + 7..][..8]
            .parse()
            .expect("offset digits");
        let len: usize = line[line.find("len=").expect("len") + 4..][..6]
            .parse()
            .expect("len digits");
        assert_eq!(
            offset,
            at + MARKER_LEN,
            "marker {i}: the span starts after the marker"
        );
        assert!(
            offset + len <= GOLDEN.len(),
            "marker {i}: the span runs past the file"
        );
        assert!(
            !GOLDEN[offset..offset + len].contains(STATEMENT_MARKER),
            "marker {i}: the span swallows another marker"
        );
    }
}

/// The reserved prefix is refused rather than escaped: a statement
/// carrying it would give the splitter an extra boundary to find.
#[test]
fn the_writer_refuses_a_statement_carrying_the_reserved_marker() {
    // The refusal is an assertion inside `render`'s `emit`, which cannot
    // be reached without a builder that emits the prefix. What IS
    // checkable here is that no statement the builders produce carries
    // it, which is the same claim from the other side.
    for line in GOLDEN.lines() {
        if let Some(rest) = line.strip_prefix(STATEMENT_MARKER) {
            assert!(
                rest.split(']')
                    .next()
                    .is_some_and(|n| n.parse::<usize>().is_ok()),
                "a marker line that is not a marker: {line}"
            );
        }
    }
}

/// Criterion 1, half one: the committed golden is the file whose digest
/// was published on the issue before any of this code existed.
#[test]
fn the_promql_statement_freeze_matches_its_committed_digest() {
    let digest = Sha256::digest(GOLDEN.as_bytes());
    assert_eq!(
        format!("{digest:x}"),
        PINNED.trim(),
        "crates/pulsus-read/tests/golden/promql_statements.txt was edited without its digest \
         (issue #548 criterion 1). The digest published on the issue is \
         1ae98d4165013f726354bc462da2e8df7c470874b0908e26a26fb56bff725d08, re-derivable by \
         running this file's `render` against 8f3348e8."
    );
}

/// The planner and the statement builders still render, byte for byte,
/// what the committed golden holds.
#[test]
fn every_statement_is_the_one_the_committed_golden_holds() {
    let rendered = render();
    if rendered != GOLDEN {
        let (a, b) = first_difference(&rendered, GOLDEN);
        panic!(
            "a statement moved from the one the committed golden holds.\n\
             rendered: {a:?}\n  golden: {b:?}\n\
             Regenerating this golden against a changed tree moves the digest away from a \
             number already published; say in the notes which query's statement moved and why."
        );
    }
}

/// The entry, line and byte counts published in the implementation notes.
///
/// A count and two sizes — exact content is criterion 1's business, and
/// criterion 1 is an external audit rather than a self-check.
#[test]
fn the_freeze_has_the_published_entry_line_and_byte_counts() {
    assert_eq!(QUERIES.len(), ENTRIES, "the generator's own query list");
    assert_eq!(
        GOLDEN.matches("== ").count(),
        ENTRIES,
        "the golden's `== ` entry count"
    );
    assert_eq!(GOLDEN.lines().count(), LINES, "the golden's line count");
    assert_eq!(GOLDEN.len(), BYTES, "the golden's byte length");
}

/// **The statement `docs/schemas.md` §2.3 prints IS the one the builder
/// renders** — the doc-consistency pattern the TraceQL SQL suites already
/// use, and the reason §2.3 can say "this is what the database receives"
/// rather than showing a transcription of it.
///
/// The literals are the ones §2.3 names in its own prose, so a reader can
/// check the block against the sentence above it, and a change to any
/// layer of the statement fails here until the document is regenerated
/// with it.
#[test]
fn the_grouped_statement_in_schemas_md_is_the_one_the_builder_renders() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root");
    let schemas = std::fs::read_to_string(root.join("docs/schemas.md")).expect("read schemas.md");
    let rendered = grouped_sql::grouped_fetch(
        SAMPLES,
        HIST,
        "http_requests_total",
        &FPS,
        &GIDS,
        Grid {
            start_ms: 1_782_907_200_000,
            step_ms: 15_000,
            points: 241,
            lookback_ms: 300_000,
        },
        1_782_906_900_000,
        1_782_910_800_000,
        grouped::GroupedOp::Max,
    );
    assert!(
        schemas.contains(&rendered),
        "docs/schemas.md §2.3 must print the grouped instant read the builder renders, byte for \
         byte. It does not; regenerate the block from the builder rather than editing it by hand."
    );
    // And the sentences beside it, which the block alone cannot carry.
    for needle in [
        "the grouped instant read (issue #549)",
        "`min`, `max`, `count` and `group`",
        "group key never enters the SQL",
        "`pushed_rows <= 2 * raw_rows`",
        "series >= 2 * groups",
    ] {
        assert!(
            schemas.contains(needle),
            "docs/schemas.md §2.3 must say {needle:?} beside the block"
        );
    }
}

/// The first line the two texts disagree on, so a failure names the byte
/// that moved rather than printing 26,727 of them.
fn first_difference<'a>(a: &'a str, b: &'a str) -> (&'a str, &'a str) {
    for (x, y) in a.lines().zip(b.lines()) {
        if x != y {
            return (x, y);
        }
    }
    ("<one text is a prefix of the other>", "")
}

/// Writes the golden and its digest. `#[ignore]`d: it is the capture
/// step, run once at the merge base, never as part of a suite.
#[test]
#[ignore = "regenerates the golden; run only to capture at a named commit"]
fn zz_regenerate_golden() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden");
    let body = render();
    let digest = Sha256::digest(body.as_bytes());
    std::fs::write(dir.join("promql_statements.txt"), &body).expect("write golden");
    std::fs::write(
        dir.join("promql_statements.sha256"),
        format!("{digest:x}\n"),
    )
    .expect("write digest");
}
