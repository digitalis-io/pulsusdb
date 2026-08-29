//! Issue #463 — the anchors that keep this issue's documented claims from
//! drifting away from the code and the numbers they describe.
//!
//! Hermetic — no server, no ClickHouse, no container. Runs under the
//! plain `ci` job's workspace test step.
//!
//! Four things are held here, and each is a claim that would otherwise
//! be prose nobody re-checks:
//!
//! * the `range-step-grid-start-anchored` ledger row's anchor block and
//!   the executable case tuple it duplicates, bound to each other;
//! * the `QuerySpec` doc block's corrected citation — the engine
//!   agreement is real, the wire claim it used to imply is false;
//! * the corpus runner's template-environment comment, which asserted a
//!   property the code does not have;
//! * the three witness-bearing ledger rows this issue adds, each of
//!   which must name the alternative it discriminates against.
//!
//! **On source-text matching.** Every assertion below normalises comment
//! continuation before it matches — a leading `\s*//[/!]?\s?` stripped
//! from each line, joined with a single space — because a doc comment
//! wraps and a phrase that reads as contiguous in the file is not
//! contiguous in its bytes. That was found by RUNNING a match, not by
//! reading one: the first version of the runner-comment check below used
//! a negative anchor that was not a substring of the file at all, so it
//! could never fire and reverting the comment would have stayed green.

use std::path::{Path, PathBuf};

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root resolves")
}

fn read(rel: &str) -> String {
    let path: PathBuf = workspace_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// Strips comment continuation so a wrapped phrase matches as one line.
fn normalise_comments(text: &str) -> String {
    let mut out = String::new();
    for line in text.lines() {
        let t = line.trim_start();
        let t = t
            .strip_prefix("///")
            .or_else(|| t.strip_prefix("//!"))
            .or_else(|| t.strip_prefix("//"))
            .unwrap_or(t);
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(t.trim());
    }
    out
}

/// The doc block immediately preceding `needle` in `src`.
fn doc_block_before(src: &str, needle: &str) -> String {
    let at = src
        .find(needle)
        .unwrap_or_else(|| panic!("{needle:?} is not in the file"));
    let head = &src[..at];
    let mut lines: Vec<&str> = Vec::new();
    for line in head.lines().rev() {
        let t = line.trim_start();
        if t.starts_with("///") || t.starts_with("#[") {
            lines.push(line);
        } else if t.is_empty() && lines.is_empty() {
            continue;
        } else {
            break;
        }
    }
    lines.reverse();
    normalise_comments(&lines.join("\n"))
}

fn anchor_block(text: &str, name: &str) -> std::collections::BTreeMap<String, i128> {
    let start = format!("<!-- {name}:start -->");
    let end = format!("<!-- {name}:end -->");
    let at = text
        .find(&start)
        .unwrap_or_else(|| panic!("the {name} anchor block is missing"));
    let body = &text[at + start.len()..];
    let body = &body[..body.find(&end).expect("the anchor block closes")];
    body.split_whitespace()
        .map(|kv| {
            let (k, v) = kv
                .split_once('=')
                .unwrap_or_else(|| panic!("bad pair {kv:?}"));
            (
                k.to_string(),
                v.parse::<i128>()
                    .unwrap_or_else(|e| panic!("{k} is not an integer: {e}")),
            )
        })
        .collect()
}

/// **The `range-grid` anchor and the executable case tuple are bound.**
///
/// The measurement exists in exactly two places after this change — the
/// anchor block in the ledger row, and the case tuple in the live suite,
/// which is the only copy that fails when the number is wrong. Editing
/// either alone reds here.
#[test]
fn the_range_grid_anchor_binds_the_executable_case() {
    let ledger = read("docs/benchmarks/logs-differential-ledger.md");
    let a = anchor_block(&ledger, "range-grid");
    for key in [
        "window_ns",
        "our_points",
        "our_step_ns",
        "ref_points",
        "our_last_offset_ns",
        "ref_last_offset_ns",
    ] {
        assert!(a.contains_key(key), "the range-grid anchor lost {key}");
    }
    // The arithmetic the row's own numbers have to satisfy.
    assert_eq!(
        a["ref_last_offset_ns"] - a["our_last_offset_ns"],
        a["our_step_ns"],
        "the reference's extra point is exactly one step past ours"
    );
    assert_eq!(
        a["ref_points"] - a["our_points"],
        1,
        "the reference emits exactly one further point"
    );
    assert_eq!(
        a["our_last_offset_ns"],
        (a["our_points"] - 1) * a["our_step_ns"],
        "our last point is `(points - 1)` steps from the start"
    );

    // The executable copy.
    let live = read("crates/pulsus-server/tests/logs_api_live.rs");
    // Rust's underscore-grouped integer spelling, so the anchor's plain
    // decimal is compared against the literal as the source writes it.
    fn grouped(n: i128) -> String {
        let d = n.to_string();
        let mut out = String::new();
        for (i, c) in d.chars().enumerate() {
            if i > 0 && (d.len() - i).is_multiple_of(3) {
                out.push('_');
            }
            out.push(c);
        }
        out
    }
    let tuple = format!(
        "({}, {}, {})",
        grouped(a["window_ns"]),
        a["our_points"],
        grouped(a["our_step_ns"]),
    );
    assert!(
        live.contains(&tuple),
        "the ledger anchor and the executable case tuple disagree — expected {tuple} in \
         logs_api_live.rs"
    );

    // And the measurement is stated ONCE: `docs/api.md` and the live
    // suite's doc comment cite the row instead of restating it.
    for (what, text) in [
        ("docs/api.md", read("docs/api.md")),
        ("logs_api_live.rs", live.clone()),
    ] {
        assert!(
            !text.contains("502s"),
            "{what} restates the grid measurement; it must cite \
             `range-step-grid-start-anchored` instead"
        );
    }
}

/// **Criterion 14a — the `QuerySpec` doc no longer attaches the engine
/// citation to a wire claim.**
///
/// The comment cited the reference's engine as authority for the
/// start-anchored grid. That is true of the ENGINE and false of the
/// WIRE, where the HTTP boundary re-anchors the request before the
/// engine ever runs. The claim had been checked at the layer that agrees
/// and written as if it described the layer that does not.
#[test]
fn the_query_spec_doc_separates_the_engine_from_the_wire() {
    let src = read("crates/pulsus-read/src/logql/params.rs");
    let block = doc_block_before(&src, "pub enum QuerySpec {");
    for anchor in [
        "splitters.go:236",
        "range-step-grid-start-anchored",
        "ENGINE-level agreement",
    ] {
        assert!(
            block.contains(anchor),
            "the QuerySpec doc block no longer carries {anchor:?}"
        );
    }
    let caveat = block
        .find("ENGINE-level agreement")
        .expect("the caveat is present");
    let mut from = 0usize;
    while let Some(at) = block[from..].find("batchRangeVectorIterator") {
        let abs = from + at;
        assert!(
            abs > caveat,
            "the engine citation appears BEFORE the caveat that scopes it — that is the \
             conflation issue #462 corrected"
        );
        from = abs + 1;
    }
}

/// **The corpus runner's template-environment comment states what the
/// code does.**
///
/// It used to say the environment is pinned "so goldens replay
/// identically on any host/CI timezone". That is true of the ZONE and
/// false of the CLOCK, and the false half is what an earlier reading of
/// this issue took as evidence that PulsusDB was insulated from
/// wall-clock drift where the reference is not. It is not; the exposure
/// is symmetric.
///
/// The negative anchor was verified ABSENT from the replacement text and
/// the positive anchors ABSENT from the forbidden text, both before this
/// check was written. What the pairing does NOT do is stop a later edit
/// from rewording the replacement back into the forbidden phrase —
/// nothing here guards that, deliberately: an instrument guarding the
/// instrument is the recursion this issue has already paid for, and the
/// cost of the uncovered case is a stale comment, not a wrong response.
#[test]
fn the_corpus_runner_comment_does_not_claim_a_pinned_clock() {
    let src = read("crates/pulsus-read/tests/logqltest/runner.rs");
    let text = normalise_comments(&src);
    assert!(
        !text.contains("Pin the template environment to the capture precondition"),
        "the runner comment claims the whole template ENVIRONMENT is pinned; only the zone is"
    );
    for anchor in [
        "Pin the template ZONE",
        "now_ns",
        "SystemTime::now()",
        "is not pinned",
    ] {
        assert!(
            text.contains(anchor),
            "the runner comment no longer carries {anchor:?}"
        );
    }
}

/// **Criterion 12 — a ledger row whose evidence is a witness frame names
/// the alternative it discriminates against.**
///
/// This is a stated requirement, not a mechanical one: the model
/// machinery that once tried to check the discrimination itself was
/// deleted after three rounds found defects in the instrument rather
/// than in what gets built. What is checked here is that each row names
/// a witness probe, names the alternative behaviour, and records the
/// residual. **Nothing mechanically prevents a later edit from
/// substituting a non-discriminating witness** — that is written into
/// each row on purpose, and re-checking it is the obligation of whoever
/// reviews a change to the witness, the alternative or the probe.
#[test]
fn every_witness_row_names_its_alternative_and_its_residual() {
    let ledger = read("docs/benchmarks/logs-differential-ledger.md");
    for (row, witness, alternative) in [
        (
            "categorize-tail-noop-pipeline",
            "`witness: T4`",
            "rename-colliding-metadata",
        ),
        (
            "tail-stream-object-granularity-unflagged",
            "`witness: T17`",
            "group-by-stream-map",
        ),
    ] {
        let at = ledger
            .find(&format!("### `{row}`"))
            .unwrap_or_else(|| panic!("the ledger has no {row} entry"));
        let body = &ledger[at..];
        let body = &body[..body[4..].find("\n### ").map_or(body.len(), |i| i + 4)];
        assert!(body.contains(witness), "{row}: no witness probe id");
        assert!(
            body.contains(alternative),
            "{row}: the alternative implementation is not named"
        );
        assert!(
            body.contains("Residual:"),
            "{row}: the residual — that nothing mechanically prevents a later edit \
             substituting a non-discriminating witness — is not written down"
        );
    }
    // The instant-log-query row is the third of this class: it pastes no
    // frame either, and binds a one-sided pair by id.
    let at = ledger
        .find("### `categorize-instant-log-query`")
        .expect("the ledger has no categorize-instant-log-query entry");
    let body = &ledger[at..];
    let body = &body[..body[4..].find("\n### ").map_or(body.len(), |i| i + 4)];
    for id in ["`F2-ref`", "`F2-pulsus`"] {
        assert!(body.contains(id), "the instant-log-query row lost {id}");
    }
}

/// The `docs/api.md` header table and the §2.1 shape both name the
/// header and the third element — the documentation half of criterion
/// 12.
#[test]
fn the_api_doc_describes_the_categorised_shape() {
    let api = read("docs/api.md");
    assert!(
        api.contains("| `X-Loki-Response-Encoding-Flags` |"),
        "the request-header table does not list the encoding-flags header"
    );
    for phrase in [
        "\"structuredMetadata\":{...},\"parsed\":{...}",
        "all-or-nothing",
        "encoding-flags-echo-order",
    ] {
        assert!(
            api.contains(phrase),
            "docs/api.md's streams section does not carry {phrase:?}"
        );
    }
}

/// Guards this file's own matcher: `normalise_comments` has to make a
/// wrapped phrase contiguous, or every assertion above passes for the
/// wrong reason.
#[test]
fn the_comment_normaliser_joins_a_wrapped_phrase() {
    let wrapped = "    /// Pin the template ZONE to the capture\n    /// precondition (stock).";
    assert!(!wrapped.contains("ZONE to the capture precondition"));
    assert!(normalise_comments(wrapped).contains("ZONE to the capture precondition"));
}

/// **Criterion 14b — every value in the client-capture anchor is
/// recomputed from the three INPUTS, and read out of the prose phrase
/// that gives it its role.**
///
/// The earlier form compared parsed inputs against constants written in
/// the test, so a prose-only edit stayed green, and it checked that the
/// numbers were PRESENT rather than that they played the right parts.
/// Both halves are fixed here: clause 1 derives, clause 3 binds each
/// value to its own sentence, so swapping the two timestamps or the two
/// point counts reds.
#[test]
fn the_grid_capture_anchor_recomputes_from_its_own_inputs() {
    let ledger = read("docs/benchmarks/logs-differential-ledger.md");
    let a = anchor_block(&ledger, "grid-capture");
    let (start, end, step) = (a["start"], a["end"], a["step_ns"]);

    // 1. Recompute all five derived values from the three inputs alone.
    let k = (end - start) / step;
    assert_eq!(a["our_last"], start + k * step, "our_last");
    assert_eq!(a["our_points"], k + 1, "our_points");
    assert_eq!(
        a["ref_extra"],
        ((end + step - 1) / step) * step,
        "ref_extra"
    );
    assert_eq!(a["ref_points"], k + 2, "ref_points");
    assert_eq!(a["past_end_ns"], a["ref_extra"] - end, "past_end_ns");

    // 2. Residues, and the unit the prose quotes.
    assert_eq!(start % step, 0, "the client's start is step-aligned");
    assert_ne!(end % step, 0, "its end is not");
    assert_eq!(step / 1_000_000, 10_000, "the prose quotes step in ms");

    // 3. Role-bound extraction: each value is read out of the phrase
    //    that gives it its meaning, not searched for anywhere in the
    //    row. Every lookup is scoped to THIS row's body, and each phrase
    //    is unique within it — a value swapped with another therefore
    //    fails its own comparison rather than being found in the other's
    //    place.
    let at = ledger
        .find("### range-step-grid-start-anchored")
        .expect("the grid row is present");
    let row = &ledger[at..];
    let row = &row[..row[4..].find("\n### ").map_or(row.len(), |i| i + 4)];
    let role = |before: &str| -> i128 {
        let at = row
            .find(before)
            .unwrap_or_else(|| panic!("the row does not carry the phrase {before:?}"));
        let rest = &row[at + before.len()..];
        let raw: String = rest
            .chars()
            .take_while(|c| c.is_ascii_digit() || *c == '.')
            .collect();
        assert!(!raw.is_empty(), "no number follows {before:?}");
        if raw.contains('.') {
            // `7.202 s` — the seconds form of `past_end_ns`.
            (raw.parse::<f64>().expect("seconds") * 1e9).round() as i128
        } else {
            raw.parse().expect("integer")
        }
    };
    assert_eq!(role("sends `start="), start, "start's role phrase");
    assert_eq!(role("`end="), end, "end's role phrase");
    assert_eq!(role("`step=") * 1_000_000, step, "step's role phrase");
    assert_eq!(
        role("up to our last point at\n  `"),
        a["our_last"],
        "our_last's role phrase"
    );
    assert_eq!(
        role("Our grid carries "),
        a["our_points"],
        "our_points' role phrase"
    );
    assert_eq!(
        role("the reference's\n  grid carries "),
        a["ref_points"],
        "ref_points' role phrase"
    );
    assert_eq!(
        role("one further point at `"),
        a["ref_extra"],
        "ref_extra's role phrase"
    );
    assert_eq!(
        role("which sits "),
        a["past_end_ns"],
        "past_end_ns' role phrase, in its seconds form"
    );

    // 4. Provenance literals.
    for literal in [
        "sha256:3fd54ae1214669f8355f065ec9f6445d5279a3d77095ab048ca045685272429b",
        "13.1.0",
    ] {
        assert!(
            ledger.contains(literal),
            "the row no longer carries the provenance literal {literal}"
        );
    }
}

fn _unused(_: &Path) {}
