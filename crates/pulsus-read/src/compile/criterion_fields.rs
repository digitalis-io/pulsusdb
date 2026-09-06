//! The criterion's own sentence drives its assertion (issue #492 part
//! 5).
//!
//! # The defect this removes
//!
//! An acceptance criterion says which fields it is about; the test
//! beside it asserts a hand-composed tuple; and whether the tuple
//! matches the sentence is a matter of inspection. Four review rounds on
//! issue #492 each found a criterion whose test reddened for a channel
//! its sentence did not name — a wire word, a stage spelling, a
//! provenance name — and each round fixed the one it found. A defect
//! beaten a new way every round is EXPRESSIBLE, and the fix is to remove
//! what expresses it.
//!
//! What expresses it is the hand-written field list. Here the list is
//! derived instead:
//!
//! ```text
//!    CRITERION_NN  (a `const` beside the test, the criterion's own sentence)
//!         |
//!         |  every [A-Za-z0-9_] run inside a backtick span
//!         v
//!      tokens  ----------- intersect -----------.
//!                                               |
//!    universe  <- serde_json::to_value(LinkShape{ .. })   <- a STRUCT LITERAL,
//!         |        every field set, so the key set is complete   so adding a field
//!         |                                                       fails to COMPILE
//!         v
//!       named = {stage, how, fidelity, why}
//!         |
//!         +--> the expectation's key set must EQUAL `named`  <- an assertion may not
//!         |                                                      read an unnamed field,
//!         |                                                      nor skip a named one
//!         +--> compare actual against expected on `named` and nothing else
//! ```
//!
//! # Where this applies, and where it does not
//!
//! It applies to an assertion whose operand is ONE object with named
//! fields. It does **not** apply to a byte freeze over a whole
//! rendering, to an inequality between two renderings, to a set
//! membership or a count, or to an assertion whose operand is a set of
//! JSON *paths*. Those four shapes keep the perturbation sweep as their
//! control.
//!
//! # What it cannot do
//!
//! It binds the assertion to the sentence **in the tree**. Nothing in CI
//! can read a GitHub comment, so a `const` that misquotes its criterion
//! yields a self-consistent test asserting the wrong thing. The plan
//! gives each `const`'s text verbatim and a reviewer diffs the two
//! strings. That gap is stated, not closed.
//!
//! # No crate-internal imports
//!
//! Only `std` and `serde_json`. An integration test reaches this file
//! through `#[path = "../src/compile/criterion_fields.rs"]`, so anything
//! it imported from `crate::` would fail to resolve there.

use std::collections::{BTreeMap, BTreeSet};

/// The fields a criterion's sentence names: every `[A-Za-z0-9_]` run
/// inside a backtick span, intersected with `universe`.
///
/// The intersection is what makes ordinary prose safe — a sentence may
/// say `` `Pipe(Aggregate)` `` or `` `("lowered", "wider", null)` ``
/// without naming a field, because none of those tokens is one. It also
/// means a sentence CAN name a field by accident: a backtick span
/// containing a lone `i` would pull `LinkShape.i` into the named set.
/// The failure direction is safe and loud — the key-set assertion fires
/// immediately with both sets printed — and the message is to be read as
/// "the sentence says too much", not "the row is missing a field".
pub fn fields_named_by(criterion: &str, universe: &BTreeSet<String>) -> BTreeSet<String> {
    let mut named = BTreeSet::new();
    let mut inside = false;
    let mut token = String::new();
    let flush = |token: &mut String, inside: bool, named: &mut BTreeSet<String>| {
        if inside && universe.contains(token.as_str()) {
            named.insert(token.clone());
        }
        token.clear();
    };
    for ch in criterion.chars() {
        if ch == '`' {
            flush(&mut token, inside, &mut named);
            inside = !inside;
            continue;
        }
        if ch.is_ascii_alphanumeric() || ch == '_' {
            token.push(ch);
        } else {
            flush(&mut token, inside, &mut named);
        }
    }
    flush(&mut token, inside, &mut named);
    named
}

/// Asserts `actual` equals `expected` on exactly the fields
/// `criterion`'s sentence names, for a JSON operand.
///
/// **An expected `null` means the wire key is ABSENT.** A wire that
/// carries an explicit `null` there fails naming the field. The two are
/// different answers to a consumer, and a lenient comparison reads them
/// as one: deleting the two `#[serde(skip_serializing_if =
/// "Option::is_none")]` attributes on `LinkShape` turns every absent key
/// into an explicit `null`, and at `ddb48c96` that change passes every
/// test in the six binaries that touch the explain surface.
///
/// # Panics
///
/// When the expectation's key set is not exactly the criterion's named
/// set, or when any named field differs.
pub fn assert_named_fields(
    criterion: &str,
    universe: &BTreeSet<String>,
    actual: &serde_json::Value,
    expected: &serde_json::Value,
    ctx: &str,
) {
    let named = fields_named_by(criterion, universe);
    let supplied: BTreeSet<String> = expected
        .as_object()
        .unwrap_or_else(|| panic!("{ctx}: the expectation must be a JSON object, got {expected}"))
        .keys()
        .cloned()
        .collect();
    assert_eq!(
        named, supplied,
        "{ctx}: the criterion names {named:?} and the expectation supplies {supplied:?} — an \
         assertion may not read a field the criterion does not name, nor omit one it does"
    );
    let actual_obj = actual
        .as_object()
        .unwrap_or_else(|| panic!("{ctx}: the actual value must be a JSON object, got {actual}"));
    for field in &named {
        let want = &expected[field];
        if want.is_null() {
            match actual_obj.get(field) {
                None => continue,
                Some(serde_json::Value::Null) => panic!(
                    "{ctx}: field `{field}`: the wire carries null, the row says the key is ABSENT"
                ),
                Some(got) => panic!(
                    "{ctx}: field `{field}`: the row says the key is ABSENT and the wire carries \
                     {got}"
                ),
            }
        }
        let got = actual_obj.get(field).unwrap_or(&serde_json::Value::Null);
        assert_eq!(got, want, "{ctx}: field `{field}`");
    }
}

/// The TOP-LEVEL fields of a `{:#?}` rendering of a derived-`Debug`
/// struct, each mapped to its rendered value.
///
/// A top-level field is a line indented by exactly four spaces whose
/// first token ends in `:`. Its value runs to just before the next such
/// line, or — for the last field — to just before the struct's own
/// closing line, which is NOT part of any field's value.
///
/// Returns an empty map for a rendering that is not the derived pretty
/// form.
pub fn fields_of_debug(pretty: &str) -> BTreeMap<String, String> {
    let lines: Vec<&str> = pretty.lines().collect();
    // Every line index that starts a top-level field.
    let starts: Vec<(usize, String)> = lines
        .iter()
        .enumerate()
        .filter_map(|(i, line)| Some((i, top_level_field_name(line)?)))
        .collect();
    let mut out = BTreeMap::new();
    for (n, (start, name)) in starts.iter().enumerate() {
        let end = match starts.get(n + 1) {
            Some((next, _)) => *next,
            // The last field stops before the struct's closing line —
            // the last line at indentation 0. Without this the final
            // field swallows the closing brace, which is how the first
            // form of this function was wrong.
            None => lines
                .iter()
                .rposition(|l| !l.is_empty() && !l.starts_with(' '))
                .unwrap_or(lines.len()),
        };
        out.insert(name.clone(), lines[*start..end].join("\n"));
    }
    out
}

/// `Some(name)` when the line opens a top-level field of a `{:#?}`
/// derived-struct rendering: exactly four spaces, an identifier, a
/// colon.
fn top_level_field_name(line: &str) -> Option<String> {
    let rest = line.strip_prefix("    ")?;
    if rest.starts_with(' ') {
        return None;
    }
    let name: String = rest
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    if name.is_empty() {
        return None;
    }
    let after = &rest[name.len()..];
    if after.starts_with(':') && !after.starts_with("::") {
        Some(name)
    } else {
        None
    }
}

/// Asserts two `{:#?}` renderings agree on exactly the top-level fields
/// `criterion`'s sentence names.
///
/// The universe is the union of both renderings' top-level field sets,
/// and every named field must be PRESENT in both. That is the
/// anti-vacuity check: if `Debug` ever stops being the derived pretty
/// form, [`fields_of_debug`] returns fewer fields and a named field goes
/// missing, so the assertion fails rather than comparing nothing. A
/// change that shortens BOTH renderings at once is not caught, and that
/// residual is stated rather than closed.
///
/// The two key sets are deliberately NOT required to be equal: a field
/// added to the struct appears on the rendered side before the pinned
/// fixture is regenerated, and this projection has to stay green through
/// exactly that window — it is what says the added field is the only
/// thing that moved.
///
/// # Panics
///
/// When the criterion names no field of either rendering, when a named
/// field is absent from either, or when any named field differs.
pub fn assert_named_debug_fields(criterion: &str, actual: &str, expected: &str, ctx: &str) {
    let a = fields_of_debug(actual);
    let e = fields_of_debug(expected);
    let universe: BTreeSet<String> = a.keys().chain(e.keys()).cloned().collect();
    let named = fields_named_by(criterion, &universe);
    assert!(
        !named.is_empty(),
        "{ctx}: the criterion names no field of the rendering; its field set is {:?}",
        universe
    );
    for field in &named {
        let got = a.get(field).unwrap_or_else(|| {
            panic!(
                "{ctx}: field `{field}` is named by the criterion and absent from the actual \
                 rendering, whose fields are {:?}",
                a.keys().collect::<Vec<_>>()
            )
        });
        let want = e.get(field).unwrap_or_else(|| {
            panic!(
                "{ctx}: field `{field}` is named by the criterion and absent from the expected \
                 rendering, whose fields are {:?}",
                e.keys().collect::<Vec<_>>()
            )
        });
        assert_eq!(got, want, "{ctx}: field `{field}`");
    }
}
