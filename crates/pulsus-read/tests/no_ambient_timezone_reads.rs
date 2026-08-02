//! Issue #311 — a **best-effort lexical tripwire** against re-introducing
//! a host-timezone read. Read the scope section before trusting it.
//!
//! ## What actually guarantees the property
//!
//! `crates/pulsus-server/tests/template_timezone_live.rs`. It boots real
//! binaries and compares rendered query output, so it observes the RESULT
//! rather than the spelling of any read.
//!
//! State the guarantee precisely, because a looser version of this
//! sentence was wrong once. That suite catches a host-timezone read that
//! **reaches the rendered output AND whose leaked value differs from the
//! expected one**. Both halves matter: a read only changes an answer when
//! the host channel it reads is non-empty, which is why that suite now
//! runs two of its three servers under a hostile `$TZ` rather than
//! trusting the runner to have one set.
//!
//! Verified, not asserted — but note *which* probe: a composed-path
//! `/etc/localtime` read planted in the template path is invisible to the
//! scan below and reddens that suite (`left: "…+0000 GMT"`). A
//! const-indirected `env::var(KEY)` read is the case that came back
//! **green** with `$TZ` unset, and is red only once the channel is
//! populated — which is exactly why the hostile-`$TZ` servers exist.
//!
//! This file is a cheap early warning that fails in `cargo test` rather
//! than in CI's live job.
//!
//! ## What this scan catches
//!
//! Exactly the direct spellings in [`BANNED`] — `env::var("TZ")`,
//! `env::var_os("TZ")`, and the `"/etc/localtime"` literal — written
//! literally, on one line.
//!
//! ## What it does NOT catch, stated so nobody relies on it
//!
//! - **Const indirection.** `const KEY: &str = "TZ"; env::var(KEY)`.
//! - **Path composition.** `Path::new("/etc").join("localtime")`.
//! - **Any read split across lines**, or spelled through a helper.
//!
//! No lexical scan can close those: the values are strings, and strings
//! can be built. Adding more patterns would enlarge the instrument without
//! changing what it proves, so the list stays at the direct spellings.
//!
//! ## The one class that IS closed properly
//!
//! Aliasing of `chrono::Local` — `use chrono::Local as HostZone;` — is
//! **not** handled here. It is denied by `disallowed-types` in
//! `clippy.toml`, which matches on the RESOLVED path and therefore sees
//! through aliases and re-exports. That is a resolution-aware instrument
//! doing what a grep cannot, so the class disappears rather than being
//! disclosed as a gap.

use std::path::{Path, PathBuf};

/// Direct code spellings that read a host timezone, with the reason each
/// is banned. Substring matches, per line.
const BANNED: &[(&str, &str)] = &[
    (
        r#"env::var("TZ")"#,
        "reads the host $TZ; the zone is reader.template_timezone",
    ),
    (
        r#"env::var_os("TZ")"#,
        "reads the host $TZ; the zone is reader.template_timezone",
    ),
    (
        r#""/etc/localtime""#,
        "reads the host zone file; the zone is reader.template_timezone",
    ),
];

/// One entry per [`BANNED`] pattern, in the same order: the pattern, then
/// **several independently written spellings** of a real read it must
/// catch.
///
/// Two properties are being defended, and they need different things.
///
/// *Independence* — the fixtures are typed out as code, never assembled
/// from the patterns. The first version of this file interpolated the
/// pattern into a string and asserted the string contained the pattern,
/// which holds for **any** string; swapping a pattern for unmatchable
/// garbage left it green. Independent fixtures make that mutation fail.
///
/// *Spelling breadth* — every entry spells its read fully qualified
/// (`std::env::var`), through an imported module (`use std::env;` then
/// `env::var`), and absolutely (`::std::env::var`); the path entry adds a
/// second API (`File::open`). A fixture set that all shared one prefix
/// could not detect the pattern being NARROWED to that prefix: with only
/// `std::env::var("TZ")` fixtures, tightening the pattern to
/// `std::env::var("TZ")` stayed green while a real `use std::env;
/// env::var("TZ")` escaped. Spanning the spellings closes that.
const OFFENDING_FIXTURES: &[(&str, &[&str])] = &[
    (
        r#"env::var("TZ")"#,
        &[
            r#"        let zone = std::env::var("TZ").unwrap_or_else(|_| "UTC".into());"#,
            "        use std::env;\n        let zone = env::var(\"TZ\").ok();",
            r#"        let zone = ::std::env::var("TZ").ok();"#,
        ],
    ),
    (
        r#"env::var_os("TZ")"#,
        &[
            r#"        if let Some(raw) = std::env::var_os("TZ") { return parse(raw); }"#,
            "        use std::env;\n        let raw = env::var_os(\"TZ\")?;",
            r#"        let raw = ::std::env::var_os("TZ")?;"#,
        ],
    ),
    (
        r#""/etc/localtime""#,
        &[
            r#"        let target = std::fs::read_link("/etc/localtime").ok()?;"#,
            "        use std::fs;\n        let target = fs::read_link(\"/etc/localtime\")?;",
            r#"        let handle = std::fs::File::open("/etc/localtime")?;"#,
        ],
    ),
];

/// Source that mentions the same subjects WITHOUT reading them — prose
/// about `$TZ`, a `TZ`-suffixed constant, the configured path — so a
/// pattern broadened until it matches everything fails here.
const CLEAN_FIXTURE: &str = r#"
//! The reference resolves the zone from the process ($TZ, else
//! /etc/localtime); this crate resolves it from configuration.
const STD_NUM_TZ: u32 = 29;
fn zone(cfg: &Config) -> Tz {
    // No host state: reader.template_timezone decides, defaulting to UTC.
    cfg.reader.template_timezone.tz()
}
"#;

/// The one matcher, shared by the tree scan and the fixture test — so the
/// fixtures exercise the code that actually guards the tree, not a
/// parallel copy of it.
fn offenders_in(text: &str, origin: &str) -> Vec<String> {
    let mut out = Vec::new();
    for (pattern, why) in BANNED {
        for (n, line) in text.lines().enumerate() {
            if line.contains(pattern) {
                out.push(format!("{origin}:{}: {pattern} — {why}", n + 1));
            }
        }
    }
    out
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root resolves")
}

/// Every `.rs` file under `dir`, recursively. Yields nothing for a
/// directory that does not exist (crates without `examples/`).
fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

fn scanned_files() -> Vec<PathBuf> {
    let root = workspace_root();
    let mut roots: Vec<PathBuf> = vec![root.join("e2e/src"), root.join("xtask/src")];
    for entry in std::fs::read_dir(root.join("crates"))
        .expect("crates/ readable")
        .flatten()
    {
        roots.push(entry.path().join("src"));
        roots.push(entry.path().join("examples"));
    }
    let mut files = Vec::new();
    for dir in &roots {
        rust_files(dir, &mut files);
    }
    files
}

#[test]
fn no_source_file_spells_a_host_timezone_read_directly() {
    let files = scanned_files();
    // The scan must be looking at something: an empty file list would make
    // the assertion below vacuously true.
    assert!(
        files.len() > 50,
        "expected the whole workspace source tree, found {} files",
        files.len()
    );

    let mut offenders: Vec<String> = Vec::new();
    for path in &files {
        let Ok(text) = std::fs::read_to_string(path) else {
            continue;
        };
        offenders.extend(offenders_in(&text, &path.display().to_string()));
    }
    assert!(
        offenders.is_empty(),
        "direct host-timezone reads found (issue #311):\n{}",
        offenders.join("\n")
    );
}

/// The scan is only evidence if it can fail. Four directions, all against
/// fixtures this test did not build from the patterns:
///
/// - the fixture table covers every pattern, in order (adding a `BANNED`
///   entry with no fixtures fails here rather than shipping unexercised);
/// - **every spelling** of every pattern is detected, by that pattern —
///   this is what resists NARROWING, e.g. tightening `env::var("TZ")` to
///   `std::env::var("TZ")` stops matching the imported-call spelling;
/// - every pattern detects at least one of its own spellings (a pattern
///   typo'd into unmatchable garbage fails here — the defect the original
///   tautological self-check let through);
/// - the clean fixture is NOT detected (a pattern broadened until it
///   matches prose fails here).
#[test]
fn the_scan_detects_real_offending_lines_and_passes_clean_ones() {
    let banned: Vec<&str> = BANNED.iter().map(|(p, _)| *p).collect();
    let fixtured: Vec<&str> = OFFENDING_FIXTURES.iter().map(|(p, _)| *p).collect();
    assert_eq!(
        banned, fixtured,
        "every banned pattern needs its own hand-written spelling set, in the same order"
    );

    for (pattern, spellings) in OFFENDING_FIXTURES {
        assert!(
            spellings.len() >= 3,
            "pattern {pattern:?} needs qualified, imported-call and absolute spellings — \
             a single-prefix set cannot detect the pattern being narrowed to that prefix"
        );
        for spelling in *spellings {
            let hits = offenders_in(spelling, "fixture");
            assert!(
                hits.iter().any(|hit| hit.contains(pattern)),
                "pattern {pattern:?} does not detect its own spelling:\n{spelling}\n\
                 (detected instead: {hits:?})"
            );
        }
    }

    let false_positives = offenders_in(CLEAN_FIXTURE, "clean");
    assert!(
        false_positives.is_empty(),
        "the clean fixture (prose about $TZ, a TZ-suffixed const, the configured path) \
         must not trip the scan: {false_positives:?}"
    );
}

/// The two instruments this file defers to, asserted rather than merely
/// described: the resolution-aware guard on `chrono::Local`, and the live
/// suite that carries the real guarantee.
#[test]
fn the_instruments_this_scan_defers_to_are_present() {
    let root = workspace_root();
    let clippy_toml = std::fs::read_to_string(root.join("clippy.toml")).expect("clippy.toml");
    assert!(
        clippy_toml.contains("disallowed-types") && clippy_toml.contains("chrono::Local"),
        "the alias-proof guard on chrono::Local must stay in clippy.toml — this scan \
         deliberately does not cover that class"
    );
    assert!(
        root.join("crates/pulsus-server/tests/template_timezone_live.rs")
            .exists(),
        "the live seam suite is what actually guarantees host-independence"
    );
    let ledger = std::fs::read_to_string(root.join("docs/benchmarks/logs-differential-ledger.md"))
        .expect("ledger readable");
    assert!(
        ledger.contains("template-timezone-configured"),
        "the configured-timezone divergence must stay on the ledger"
    );
}
