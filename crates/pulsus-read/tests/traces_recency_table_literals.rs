//! Issue #560: the numbers the two derived trace tables' views carry as
//! TEXT, bound to the numbers the reader carries as CODE — and the
//! sentences in the documents that describe those tables.
//!
//! ```text
//!   crates/pulsus-schema/src/catalog.rs        (text, read here)
//!     trace_recent_mv        intDiv(timestamp_ns, 300000000000)   <-+
//!     trace_error_spans_mv   WHERE status_code = 2                <-|-+
//!                                                                   | |
//!   crates/pulsus-read                                              | |
//!     window_sql::RECENT_BUCKET_NS  = 300_000_000_000  -------------+ |
//!     { status != error } renders `status_code != 2`  ----------------+
//! ```
//!
//! `pulsus-read` has no production dependency on `pulsus-schema`, and
//! `pulsus-schema`'s catalog module is private, so the view templates are
//! read as source text. Reading a file needs no dependency edge.
//!
//! The reader's error code is taken from `{ status != error }` rather
//! than `{ status = error }`, because after issue #560 the second renders
//! no `status_code` term at all (it reads `trace_error_spans`, whose view
//! IS the predicate), while the first keeps its `trace_spans` generator.

use pulsus_read::traces::compile_span_filter;
use pulsus_read::traces::window_sql::RECENT_BUCKET_NS;
use pulsus_traceql::{SpansetExpr, SpansetFilter, parse};

const CATALOG: &str = "crates/pulsus-schema/src/catalog.rs";
const SCHEMAS_MD: &str = "docs/schemas.md";
const ARCHITECTURE_MD: &str = "docs/architecture.md";

fn repo_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

fn read(rel: &str) -> String {
    std::fs::read_to_string(repo_root().join(rel)).unwrap_or_else(|e| panic!("read {rel}: {e}"))
}

fn first_filter(query: &str) -> SpansetFilter {
    match parse(query).expect("the query parses").spanset {
        SpansetExpr::Filter(filter) => filter,
        other => panic!("expected a single spanset filter for {query}, got {other:?}"),
    }
}

/// The source text of the `MvDef` whose `name:` is `name`: from that
/// field to the next `MvDef {` (or the end of the file). `None` when no
/// such definition exists.
fn mv_def_text<'a>(catalog: &'a str, name: &str) -> Option<&'a str> {
    let needle = format!("name: \"{name}\"");
    let start = catalog.find(&needle)?;
    let rest = &catalog[start..];
    let end = rest.find("MvDef {").unwrap_or(rest.len());
    Some(&rest[..end])
}

/// The unsigned integer that follows `prefix` in `text`.
fn integer_after(text: &str, prefix: &str) -> Option<i64> {
    let at = text.find(prefix)? + prefix.len();
    let digits: String = text[at..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().ok()
}

/// Every whitespace run collapsed to one space.
fn collapse(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[test]
fn the_recency_view_divides_by_the_readers_bucket_width() {
    let catalog = read(CATALOG);
    let def = mv_def_text(&catalog, "trace_recent_mv").unwrap_or_else(|| {
        panic!(
            "no MvDef named \"trace_recent_mv\" in {CATALOG}; the reader's bucket width \
             RECENT_BUCKET_NS = {RECENT_BUCKET_NS} (crates/pulsus-read/src/traces/window_sql.rs) \
             has no view to agree with"
        )
    });
    let n = integer_after(def, "intDiv(timestamp_ns, ").unwrap_or_else(|| {
        panic!("trace_recent_mv in {CATALOG} carries no `intDiv(timestamp_ns, <n>)`:\n{def}")
    });
    assert_eq!(
        n, RECENT_BUCKET_NS,
        "trace_recent_mv in {CATALOG} buckets by {n} ns; the reader's RECENT_BUCKET_NS in \
         crates/pulsus-read/src/traces/window_sql.rs is {RECENT_BUCKET_NS} ns — the two must be \
         the same number or the bucket clause prunes the wrong buckets"
    );
}

#[test]
fn the_error_view_filters_on_the_code_the_reader_renders_for_error() {
    let compiled =
        compile_span_filter(&first_filter("{ status != error }")).expect("the filter compiles");
    let predicate = &compiled.generators[0].predicate;
    let m = integer_after(predicate, "status_code != ").unwrap_or_else(|| {
        panic!("`{{ status != error }}` must render `status_code != <m>`, got {predicate:?}")
    });

    let catalog = read(CATALOG);
    let def = mv_def_text(&catalog, "trace_error_spans_mv").unwrap_or_else(|| {
        panic!(
            "no MvDef named \"trace_error_spans_mv\" in {CATALOG}; the reader's error code {m} \
             has no view to agree with"
        )
    });
    let n = integer_after(def, "WHERE status_code = ").unwrap_or_else(|| {
        panic!("trace_error_spans_mv in {CATALOG} carries no `WHERE status_code = <n>`:\n{def}")
    });
    assert_eq!(
        n,
        i64::from(m),
        "trace_error_spans_mv in {CATALOG} keeps spans with status_code = {n}; the reader \
         renders status_code != {m} for `{{ status != error }}`"
    );
}

#[test]
fn a_recency_bucket_never_straddles_a_utc_day() {
    let rem = 86_400_000_000_000_i64.checked_rem(RECENT_BUCKET_NS);
    assert_eq!(
        rem,
        Some(0),
        "a UTC day of 86,400,000,000,000 ns must be a whole number of buckets of \
         RECENT_BUCKET_NS = {RECENT_BUCKET_NS} ns, or a bucket crosses midnight and \
         `date` is not a function of `bucket`"
    );
    assert_eq!(RECENT_BUCKET_NS, 300_000_000_000);
}

#[test]
fn the_architecture_sharding_bullet_names_both_new_tables() {
    let doc = collapse(&read(ARCHITECTURE_MD));
    let needle = collapse(
        "- `trace_spans`, `trace_attrs_idx`, `trace_recent`, `trace_error_spans`: \
         `cityHash64(trace_id)` — a trace is whole on one shard; span-level intersections are \
         shard-local, and the two derived trace tables sit on the shard that holds the spans \
         they are written from.",
    );
    assert!(
        doc.contains(&needle),
        "{ARCHITECTURE_MD} must carry the trace-family sharding bullet naming both derived \
         tables:\n{needle}"
    );
}

/// The paragraph that begins with the line `lead` and ends at the next
/// blank line, whitespace collapsed. `None` when no line is `lead`.
fn paragraph_from(doc: &str, lead: &str) -> Option<String> {
    let mut lines = doc.lines();
    // The lead line itself may carry more text after the bold lead.
    let first = lines.by_ref().find(|l| l.trim_start().starts_with(lead))?;
    let mut out = vec![first.to_string()];
    for l in lines {
        if l.trim().is_empty() {
            break;
        }
        out.push(l.to_string());
    }
    Some(collapse(&out.join("\n")))
}

/// The lines between the heading starting `## 7.` and the next `## `
/// heading, each whitespace-collapsed.
fn section_seven_lines(doc: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut inside = false;
    for l in doc.lines() {
        if l.starts_with("## ") {
            if inside {
                break;
            }
            inside = l.starts_with("## 7.");
            continue;
        }
        if inside {
            out.push(collapse(l));
        }
    }
    out
}

#[test]
fn the_documents_state_what_a_failing_view_leaves_behind() {
    let mut missing: Vec<String> = Vec::new();

    let schemas = read(SCHEMAS_MD);
    match paragraph_from(&schemas, "**What a failing view leaves behind (#560).**") {
        None => missing.push(format!(
            "{SCHEMAS_MD}: no paragraph beginning `**What a failing view leaves behind (#560).**`"
        )),
        Some(p) => {
            let sentence = collapse(
                "The insert fails: the caller receives `HTTP 500` with `Code: 395`, and is not \
                 told which tables kept the block's rows.",
            );
            for needle in [
                sentence.as_str(),
                "300 of 300",
                "297 of 300",
                "28, 25 and 32",
                "7 times",
                "0.25",
                "docs/traceql-schema-migration.md",
            ] {
                if !p.contains(needle) {
                    missing.push(format!(
                        "{SCHEMAS_MD} failing-view paragraph lacks {needle:?}"
                    ));
                }
            }
        }
    }
    let seven = section_seven_lines(&schemas);
    if !seven
        .iter()
        .any(|l| l.contains("failing view") && l.contains("§4.1"))
    {
        missing.push(format!(
            "{SCHEMAS_MD} §7: no line holding both `failing view` and `§4.1`"
        ));
    }

    let architecture = read(ARCHITECTURE_MD);
    match paragraph_from(&architecture, "**A failing view (#560).**") {
        None => missing.push(format!(
            "{ARCHITECTURE_MD}: no paragraph beginning `**A failing view (#560).**`"
        )),
        Some(p) => {
            for needle in ["The insert fails", "docs/schemas.md"] {
                if !p.contains(needle) {
                    missing.push(format!(
                        "{ARCHITECTURE_MD} failing-view paragraph lacks {needle:?}"
                    ));
                }
            }
        }
    }

    assert!(
        missing.is_empty(),
        "{} missing:\n{}",
        missing.len(),
        missing.join("\n")
    );
}
