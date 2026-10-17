//! The TraceQL regex invariant's structural gate (issue #282).
//!
//! **rustc** owns the reach half, and it is the real seal: the raw
//! escapers are private to `logql::escape`, so no code in `traces/` can
//! render a regex except through `ch_regex_anchored_checked`. That is a
//! compile error (`E0425`/`E0603`), not a convention.
//!
//! **This file** pins the consumer side: no capability token for the raw
//! escapers comes back into the traces sources. The two gates that pinned
//! the previous search engine's private regex-compile leaf
//! (`search_plan::eval_compile`) went with that engine, #602 part 2c-2b-2.

use std::path::Path;

fn read(rel: &str) -> String {
    let p = Path::new(env!("CARGO_MANIFEST_DIR")).join(rel);
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("read {p:?}: {e}"))
}

/// TraceQL holds no capability token for the raw escapers any more
/// (issue #282's headline). `logql::escape`'s own surface gate (check D
/// in `logqltest_provenance.rs`) pins the escape module; this pins the
/// consumer side, so re-adding a token here is caught even if the
/// escape-module table is edited to match.
#[test]
fn traces_holds_no_capability_token_for_the_raw_escapers() {
    for rel in [
        "src/traces/mod.rs",
        "src/traces/filter.rs",
        "src/traces/metrics_sql.rs",
    ] {
        let text = read(rel);
        for banned in [
            "TraceqlPrevalidated",
            "ch_regex_anchored_traceql_prevalidated",
        ] {
            // Skip comment lines: the module docs legitimately record
            // that the token was DELETED.
            let hit = text
                .lines()
                .find(|l| !l.trim_start().starts_with("//") && l.contains(banned));
            assert!(
                hit.is_none(),
                "{rel}: {banned:?} is back in code: {:?}",
                hit.unwrap()
            );
        }
    }
}
