//! Issue #492 part 6: **the four group-2 selector classes, and the two
//! claims the measurement left behind.**
//!
//! Part 6 measured four TraceQL selector classes that
//! `docs/query-lowering.md` §3.3 listed as "could be lowered, has not
//! been", and it lowered none of them. The outcome is a record, not a
//! code change — see `docs/query-lowering.md` §9.7. Two sentences in
//! that record are about what the compiler emits today, and this file is
//! what makes them fail if they stop being true.
//!
//! # Claim 1 — a negated `name` leaf is a bounded span scan, not the window
//!
//! §3.3 used to say `{ name != "x" }` "widens the candidate generator to
//! the whole window (`GenClass::TimeRange`)". It does not.
//! `compile_leaf` routes a physical predicate through
//! `spans_generator_for` (`crates/pulsus-read/src/traces/filter.rs:2296`),
//! which always returns `GenClass::SpanScan` with the predicate rendered
//! into the `WHERE`. The construct that does produce the empty-predicate
//! time-range superset is a negated **attribute** leaf, `{ .a != "5" }`.
//! §3.3 and §3.4 now say that, and
//! [`a_negated_physical_leaf_keeps_its_predicate_a_negated_attribute_leaf_does_not`]
//! is the check.
//!
//! The two are asserted together on purpose. A true sentence about one
//! of them is exactly what made the other one wrong.
//!
//! # Claim 2 — no generator predicate carries a sub-statement
//!
//! The push part 6 measured and refused is a per-span pre-grouping:
//!
//! ```text
//!   ... AND (key = 'a')
//!   AND trace_id IN (
//!     SELECT trace_id FROM trace_attrs_idx ...
//!     GROUP BY trace_id, span_id
//!     HAVING countIf(key = 'a') > 0 AND ... maxIf(val_num, key = 'a') >= ...
//!   )
//! ```
//!
//! It is correct and it saves a great deal on the metered hop, and it is
//! refused because it needs more memory than the shipped generator
//! budget allows on the current attribute index order — §9.7 carries the
//! numbers and the two settings that decide them.
//!
//! [`no_generator_predicate_carries_a_sub_statement`] is that refusal
//! written as something that can fail. If a later change lowers a
//! cross-attribute comparison into the phase-1 statement, this test goes
//! red and the change has to come back to §9.7 and say what moved.
//!
//! # Where this stops
//!
//! Both tests read `compile_span_filter` and nothing else. They say what
//! the compiler emits; they say nothing about what the emitted statement
//! costs, and nothing in this file can observe a memory ceiling. Every
//! figure in §9.7 was taken on a 71,000,000-row developer corpus that no
//! longer exists, and none of it is checkable in CI. The recipe in §9.7
//! is what makes those figures re-takeable; this file is not.

use pulsus_read::traces::compile_span_filter;
use pulsus_read::traces::filter::{CompiledSpanFilter, GenClass};
use pulsus_traceql::{SpansetExpr, SpansetFilter, parse};

/// Parses a query that is a single `{...}` spanset filter and returns
/// that filter. Panics on anything else, because every query in this
/// file is written as one.
fn first_filter(query: &str) -> SpansetFilter {
    match parse(query).expect("the query parses").spanset {
        SpansetExpr::Filter(filter) => filter,
        other => panic!("expected a single spanset filter for {query}, got {other:?}"),
    }
}

fn compile(query: &str) -> CompiledSpanFilter {
    compile_span_filter(&first_filter(query)).expect("the filter compiles")
}

/// `{ name != "x" }` generates a bounded span scan carrying its own
/// predicate; `{ .a != "5" }` generates the empty-predicate time-range
/// superset. §3.3 of `docs/query-lowering.md` attributed the second
/// behaviour to the first construct until part 6 measured it.
#[test]
fn a_negated_physical_leaf_keeps_its_predicate_a_negated_attribute_leaf_does_not() {
    let physical = compile(r#"{ name != "x" }"#);
    assert_eq!(
        physical.generators.len(),
        1,
        "a negated name leaf contributes one generator"
    );
    let physical = &physical.generators[0];
    assert_eq!(
        physical.class,
        GenClass::SpanScan,
        "a negated physical leaf generates a bounded span scan, not the time-range superset"
    );
    assert_eq!(
        physical.predicate, "name != 'x'",
        "the negated physical predicate is rendered into the generator's WHERE"
    );

    let attribute = compile(r#"{ .a != "5" }"#);
    assert_eq!(
        attribute.generators.len(),
        1,
        "a negated attribute leaf contributes one generator"
    );
    let attribute = &attribute.generators[0];
    assert_eq!(
        attribute.class,
        GenClass::TimeRange,
        "a negated attribute leaf is the construct that widens to the whole window"
    );
    assert_eq!(
        attribute.predicate, "",
        "the time-range superset carries no predicate of its own"
    );
}

/// No phase-1 generator predicate contains a nested statement. This is
/// part 6's refusal of the per-span pre-grouping, expressed as a check
/// that a later lowering would break.
#[test]
fn no_generator_predicate_carries_a_sub_statement() {
    // One query per group-2 class part 6 measured, plus the class-D
    // physical leaf it reclassified and a group-3 attribute equality as
    // the control that a lowered leaf still has no sub-statement.
    let queries = [
        r#"{ .a = .b }"#,
        r#"{ .a * 2 > .b }"#,
        r#"{ .a = event:name }"#,
        r#"{ .s1 = .s2 }"#,
        r#"{ name != "x" }"#,
        r#"{ span.a = "1" }"#,
    ];

    let mut seen = 0usize;
    for query in queries {
        for generator in compile(query).generators {
            seen += 1;
            let lowered = generator.predicate.to_ascii_lowercase();
            for fragment in ["select", "group by"] {
                assert!(
                    !lowered.contains(fragment),
                    "{query}: a generator predicate carries a sub-statement, which is the \
                     per-span pre-grouping issue #492 part 6 refused on memory: {}",
                    generator.predicate
                );
            }
        }
    }
    assert_eq!(
        seen, 6,
        "each of the six queries contributes exactly one generator; a different count means the \
         generator set moved and the loop above no longer covers what it names"
    );
}
