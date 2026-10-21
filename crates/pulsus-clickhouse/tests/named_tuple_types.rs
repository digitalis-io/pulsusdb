//! The vendored type parser and a **named** tuple (issue #585).
//!
//! `vendor/clickhouse-types` is a `[patch.crates-io]` path source and not a
//! workspace member, so a `#[test]` written inside it is never compiled by
//! `cargo test --workspace`. These cases therefore live here, in a workspace
//! crate that takes `clickhouse-types` as a dev-dependency — the only door to
//! `DataTypeNode` from this workspace, because `clickhouse` does not
//! re-export it.
//!
//! Two things the parser must do that upstream's cannot:
//!
//! * read a named tuple at any depth. `DESCRIBE TABLE` returns the **pretty**
//!   name, so an element arrives with a newline and four spaces before it, and
//!   a SELECT response header carries the **compact** one; both forms must
//!   parse, and `Display` must emit the compact one, because the server
//!   compares an insert header's type string against its own `getName()` byte
//!   for byte;
//! * return rather than abort on bytes off the wire. `DataTypeNode::new`
//!   sliced its input at fixed byte offsets without checking either end is a
//!   character boundary, and 103 inputs of the 1,073-input corpus below
//!   aborted the caller at 16 distinct sites.
//!
//! **No case here names the new enum variant or its payload type.** Every one
//! asserts through `Display` or through the error text, so the whole file
//! compiles against the unpatched copy and each case fails on its own
//! assertion rather than on a missing symbol.
//!
//! Hermetic: no server, no network, no gate.

use clickhouse_types::DataTypeNode;

// ---------------------------------------------------------------------
// The three assertion shapes.
// ---------------------------------------------------------------------

/// The input parses and renders to `expected`, byte for byte.
#[track_caller]
fn renders(input: &str, expected: &str) {
    let got = DataTypeNode::new(input)
        .map(|v| v.to_string())
        .map_err(|e| e.to_string());
    assert_eq!(
        got,
        Ok(expected.to_string()),
        "DataTypeNode::new({input:?})"
    );
}

/// The input is refused with an error whose text contains `needle`.
#[track_caller]
fn errs(input: &str, needle: &str) {
    let got = DataTypeNode::new(input);
    let msg = got
        .as_ref()
        .err()
        .map(ToString::to_string)
        .unwrap_or_default();
    assert!(
        msg.contains(needle),
        "DataTypeNode::new({input:?}) gave {got:?}; wanted an error containing {needle:?}"
    );
}

/// How one call came out: a value, an error, or an abort.
fn outcome(input: &str) -> Result<Result<DataTypeNode, String>, ()> {
    match std::panic::catch_unwind(|| DataTypeNode::new(input)) {
        Ok(Ok(v)) => Ok(Ok(v)),
        Ok(Err(e)) => Ok(Err(e.to_string())),
        Err(_) => Err(()),
    }
}

fn describe(got: &Result<Result<DataTypeNode, String>, ()>) -> String {
    match got {
        Ok(Ok(v)) => format!("Ok({v})"),
        Ok(Err(e)) => format!("Err({e})"),
        Err(()) => "panicked".to_string(),
    }
}

/// The input is refused with an error containing `needle`, and an abort fails
/// the assertion rather than replacing it. For the inputs that abort before
/// the patch, where neither shape above would name the input or the
/// expectation.
#[track_caller]
fn errs_without_aborting(input: &str, needle: &str) {
    let got = outcome(input);
    assert!(
        matches!(&got, Ok(Err(e)) if e.contains(needle)),
        "DataTypeNode::new({input:?}) gave {}; wanted an error containing {needle:?}",
        describe(&got)
    );
}

/// The input is refused with **an** error — neither a value nor an abort.
#[track_caller]
fn errs_at_all_without_aborting(input: &str, site: &str) {
    let got = outcome(input);
    assert!(
        matches!(&got, Ok(Err(_))),
        "DataTypeNode::new({input:?}) gave {}; wanted an error, at the checked index {site}",
        describe(&got)
    );
}

// ---------------------------------------------------------------------
// The two approved column types, in both forms.
// ---------------------------------------------------------------------

/// The pretty form `DESCRIBE TABLE` returns for `trace_landing.events`.
const EVENTS_PRETTY: &str = "Array(Tuple(\n    time_ns Int64,\n    name LowCardinality(String),\n    attrs JSON,\n    dropped_attrs UInt32))";
/// The compact form a SELECT response header carries for the same column, and
/// the string the server compares an insert header against.
const EVENTS_COMPACT: &str =
    "Array(Tuple(time_ns Int64, name LowCardinality(String), attrs JSON, dropped_attrs UInt32))";
/// The pretty form for `trace_landing.links`.
const LINKS_PRETTY: &str = "Array(Tuple(\n    trace_id FixedString(16),\n    span_id FixedString(8),\n    trace_state String,\n    flags UInt32,\n    attrs JSON,\n    dropped_attrs UInt32))";
/// The compact form for the same column.
const LINKS_COMPACT: &str = "Array(Tuple(trace_id FixedString(16), span_id FixedString(8), trace_state String, flags UInt32, attrs JSON, dropped_attrs UInt32))";

// ---------------------------------------------------------------------
// N1-N9: the named reading.
// ---------------------------------------------------------------------

/// **N1.** The shape the whole change is about.
#[test]
fn n1_a_named_tuple_parses_and_renders_to_itself() {
    renders("Tuple(a Int64, b String)", "Tuple(a Int64, b String)");
}

/// **N2.** The pretty form in, the compact form out — R1 and R3 in one case.
/// The two expected strings are what the server compares the insert header
/// against.
#[test]
fn n2_the_pretty_form_parses_and_renders_compact() {
    renders(EVENTS_PRETTY, EVENTS_COMPACT);
    renders(LINKS_PRETTY, LINKS_COMPACT);
}

/// **N3.** The compact form parses too — R2, the read path — and the two forms
/// are one value.
///
/// The `is_ok` assertions come first: the equality alone holds before the
/// patch, because both sides are `None`.
#[test]
fn n3_the_compact_form_parses_to_the_same_value_as_the_pretty_one() {
    for (pretty, compact) in [
        (EVENTS_PRETTY, EVENTS_COMPACT),
        (LINKS_PRETTY, LINKS_COMPACT),
    ] {
        let compact_parse = DataTypeNode::new(compact);
        assert!(
            compact_parse.is_ok(),
            "DataTypeNode::new({compact:?}) gave {compact_parse:?}; wanted a value"
        );
        let pretty_parse = DataTypeNode::new(pretty);
        assert!(
            pretty_parse.is_ok(),
            "DataTypeNode::new({pretty:?}) gave {pretty_parse:?}; wanted a value"
        );
        assert_eq!(
            compact_parse.ok(),
            pretty_parse.ok(),
            "the two forms of one column type are not one value"
        );
    }
}

/// **N4.** A comma inside a back-quoted element name, and an escaped back
/// quote. The name token is kept verbatim, with its back quotes and escapes.
#[test]
fn n4_a_comma_and_an_escape_inside_a_back_quoted_name() {
    let input = r"Tuple(`a,b` String, `c\`d` Int8)";
    renders(input, input);
}

/// **N5.** A parenthesis inside a back-quoted name. Upstream's scanner never
/// returns its parenthesis counter to zero here, so it yields no argument at
/// all.
#[test]
fn n5_a_parenthesis_inside_a_back_quoted_name() {
    let input = "Tuple(`a(b` String)";
    renders(input, input);
}

/// **N6.** A single quote inside a back-quoted name, with a second,
/// bare-named element. Upstream's scanner opens a quoted region at the `'` and
/// swallows the top-level comma.
#[test]
fn n6_a_single_quote_inside_a_back_quoted_name() {
    let input = "Tuple(`a'b` String, c UInt8)";
    renders(input, input);
}

/// **N7.** A name the server must back-quote, because it is one of the four
/// excluded keywords.
#[test]
fn n7_a_back_quoted_keyword_name() {
    let input = "Tuple(`select` String)";
    renders(input, input);
}

/// **N8.** A named tuple is **not** equal to the positional tuple of the same
/// element types.
#[test]
fn n8_a_named_tuple_is_not_the_positional_tuple_of_the_same_types() {
    let named = DataTypeNode::new("Tuple(a Int64)");
    assert!(
        named.is_ok(),
        "DataTypeNode::new(\"Tuple(a Int64)\") gave {named:?}; wanted a value"
    );
    let positional = DataTypeNode::new("Tuple(Int64)");
    assert!(
        positional.is_ok(),
        "DataTypeNode::new(\"Tuple(Int64)\") gave {positional:?}; wanted a value"
    );
    assert_ne!(
        named.ok(),
        positional.ok(),
        "a named tuple compares equal to the positional tuple of the same element types"
    );
}

/// **N9.** An element whose **name** is one of the tail-accepting dispatch
/// arm keys, and whose type text round-trips.
#[test]
fn n9_an_element_named_for_a_dispatch_arm_key() {
    renders("Tuple(Variant Int64)", "Tuple(Variant Int64)");
}

// ---------------------------------------------------------------------
// N10-N13, N15, N16, N18, N20: the pins. Green before the change, each tied
// to one deliberate break of the line it guards.
// ---------------------------------------------------------------------

/// **N10a** *(pin)*. One member of each of the five counterexample families:
/// a first argument whose type half re-renders differently, **and** a second
/// argument with no name token. Two independent reasons to fall back, which is
/// why these five pin the fallback and not the re-rendering test.
#[test]
fn n10a_the_five_counterexample_families_keep_their_value() {
    renders("Tuple(Array Timex, UInt8)", "Tuple(Array(Time), UInt8)");
    renders(
        "Tuple(Nullable Timex, UInt8)",
        "Tuple(Nullable(Time), UInt8)",
    );
    renders(
        "Tuple(LowCardinality Timex, UInt8)",
        "Tuple(LowCardinality(Time), UInt8)",
    );
    renders("Tuple(Tuple Timex, UInt8)", "Tuple(Tuple(Time), UInt8)");
    renders("Tuple(Variant Timex, UInt8)", "Tuple(Variant(Time), UInt8)");
}

/// **N10b** *(pin)*. The same five families with the nameless argument
/// removed, so the fallback is reached **only** through the re-rendering test:
/// `Timex` parses as `Time`, which renders `Time`, not `Timex`.
#[test]
fn n10b_the_re_rendering_test_is_what_keeps_these_five() {
    renders("Tuple(Array Timex)", "Tuple(Array(Time))");
    renders("Tuple(Nullable Timex)", "Tuple(Nullable(Time))");
    renders("Tuple(LowCardinality Timex)", "Tuple(LowCardinality(Time))");
    renders("Tuple(Tuple Timex)", "Tuple(Tuple(Time))");
    renders("Tuple(Variant Timex)", "Tuple(Variant(Time))");
}

/// **N11** *(pin)*. The three arms that accept a trailing tail with no
/// composition at all.
#[test]
fn n11_the_three_tail_accepting_arms_keep_their_values() {
    renders("Tuple(Time foo)", "Tuple(Time)");
    renders("Tuple(Variant x)", "Tuple(Variant())");
    renders("Tuple(DateTime foobar)", "Tuple(DateTime('oob'))");
}

/// **N12** *(pin)*. A mixed argument list takes today's path, so the message
/// is today's: a tuple is wholly named or wholly positional and the server
/// cannot emit a mixed one either way.
#[test]
fn n12_a_mixed_argument_list_reports_todays_message() {
    errs("Tuple(a Int64, String)", "Unknown data type: a Int64");
    errs("Tuple(Int64, b String)", "Unknown data type: b String");
}

/// **N13** *(pin)*. An unpaired back quote outside any name. The pin on the
/// existing splitter keeping every byte: a shared back-quote-aware scanner
/// swallows the top-level comma here and loses the second element silently.
#[test]
fn n13_an_unpaired_back_quote_keeps_both_elements() {
    renders("Tuple(Array Time`x, UInt8)", "Tuple(Array(Time), UInt8)");
}

/// **N14.** The value changes: each of these parses today as something else,
/// and each expected string is the input string.
#[test]
fn n14_a_name_token_that_is_a_dispatch_key_reads_as_a_name() {
    renders("Tuple(Time String)", "Tuple(Time String)");
    renders("Tuple(Timestamp UInt64)", "Tuple(Timestamp UInt64)");
    renders(
        "Tuple(Array DateTime('UTC'))",
        "Tuple(Array DateTime('UTC'))",
    );
}

/// **N15** *(pin)*. The identifier test, in both directions: without it all
/// three become accepted named tuples.
#[test]
fn n15_a_name_token_that_is_not_an_identifier_is_not_a_name() {
    errs("Tuple(1a Int64)", "Unknown data type: 1a Int64");
    errs("Tuple(a-b Int64)", "Unknown data type: a-b Int64");
    errs("Tuple(Array(UInt8) Time)", "Unknown data type: UInt8) Tim");
}

/// **N16** *(pin)*. The one pin in this file that guards against an **abort**
/// rather than a wrong value: without the `parse_json` guard the named reading
/// reaches an unchecked index and all three abort.
#[test]
fn n16_a_json_element_with_a_parameterless_path_is_an_error() {
    errs_without_aborting("Tuple(a JSON(x))", "Unknown data type: a JSON(x)");
    errs_without_aborting(
        "Tuple(a JSON( Array(UInt8)))",
        "Unknown data type: a JSON( Array(UInt8))",
    );
    errs_without_aborting("Tuple(a JSON())", "Unknown data type: a JSON()");
}

/// **N17.** The `JSON` path-and-type split, at top level. All four abort
/// before the change, so the assertion is on the outcome and a panic fails it
/// rather than replacing it.
#[test]
fn n17_a_json_parameter_without_a_type_is_an_error() {
    for input in ["JSON(x)", "JSON()", "JSON(a)", "JSON( Array(UInt8)"] {
        errs_without_aborting(input, "Invalid JSON format, expected a path and its type");
    }
}

/// **N18** *(pin)*. An unclosed parenthesis, so the splitter's final push is
/// skipped and the argument list is empty. This, and not `Tuple()`, is the
/// input that reaches the emptiness guard.
#[test]
fn n18_an_unclosed_parenthesis_yields_an_empty_argument_list() {
    errs(
        "Tuple(()",
        "Expected at least one inner element in a Tuple from input Tuple(()",
    );
}

/// **N19.** The canonicalisation, which is why byte identity is not claimed:
/// every argument comes back verbatim and the separator is rewritten to the
/// server's own `", "`.
#[test]
fn n19_the_separator_is_canonicalised_and_the_arguments_are_not() {
    renders("Tuple(a Int64,b String)", "Tuple(a Int64, b String)");
    renders("Tuple( a Int64, b String)", "Tuple(a Int64, b String)");
    renders("Tuple(a Int64,\n    b String)", "Tuple(a Int64, b String)");
}

/// **N20** *(pin)*. Whitespace **inside** an argument is refused, not
/// canonicalised: at all three the type half fails to parse. Note the
/// trailing space in the last two needles.
#[test]
fn n20_whitespace_inside_an_argument_is_refused() {
    errs("Tuple(a  Int64)", "Unknown data type: a  Int64");
    errs("Tuple(a Int64 , b String)", "Unknown data type: a Int64 ");
    errs(
        "Tuple(a Array(UInt8) , b Int8)",
        "Unknown data type: a Array(UInt8) ",
    );
}

/// **N21.** The fixed point: whatever the first rendering is, parsing it again
/// renders the same bytes. Six of these twelve are errors before the change,
/// which is where it is red.
#[test]
fn n21_a_rendered_type_string_is_a_fixed_point() {
    for input in [
        "Tuple(a Int64, b String)",
        "Tuple(`a,b` String)",
        "Tuple(Variant Int64)",
        "Tuple(Time String)",
        "Tuple(Timestamp UInt64)",
        "Tuple(Array DateTime('UTC'))",
        "Tuple(a Int64,b String)",
        "Tuple( a Int64, b String)",
        "Tuple(a Int64,\n    b String)",
        "Tuple(Array Timex, UInt8)",
        "Tuple(Array Timex)",
        "Tuple(Time foo)",
    ] {
        let first = DataTypeNode::new(input);
        assert!(
            first.is_ok(),
            "DataTypeNode::new({input:?}) gave {first:?}; wanted a value"
        );
        let once = first.expect("asserted above").to_string();
        let second = DataTypeNode::new(&once)
            .map(|v| v.to_string())
            .map_err(|e| e.to_string());
        assert_eq!(
            second,
            Ok(once.clone()),
            "re-parsing {once:?}, the rendering of {input:?}, did not give the same bytes"
        );
    }
}

// ---------------------------------------------------------------------
// N22, N23: the parser is total.
// ---------------------------------------------------------------------

/// One complete, valid type per prefix arm, plus the forms carrying a second
/// parameter or a quoted region — where a byte offset can land inside a
/// character further right than the arm key.
const BASES: &[&str] = &[
    "JSON(a Int8)",
    "Decimal(9, 2)",
    "DateTime64(3)",
    "DateTime64(3, 'UTC')",
    "DateTime('UTC')",
    "Time64(3)",
    "Time",
    "IntervalDay",
    "Nullable(String)",
    "LowCardinality(String)",
    "FixedString(16)",
    "Array(UInt8)",
    "Enum8('a' = 1)",
    "Enum16('a' = 1, 'b' = 2)",
    "Enum8('a' = 1, '' = 42)",
    "Enum8('f\\'()' = 1)",
    "Map(String, UInt8)",
    "Tuple(UInt8, String)",
    "Tuple(a Int64, b String)",
    "Variant(UInt8, String)",
    "SimpleAggregateFunction(min, UInt32)",
];

/// The three inputs the generator cannot produce: a `JSON` parameter with no
/// space in it. No truncation or insertion of a valid `JSON(a Int8)` has one.
const NAMED: &[&str] = &["JSON(x)", "JSON()", "JSON(a)"];

/// One two-byte and one three-byte character inserted at every character
/// boundary of each base, every truncation of each base at every byte, and the
/// three named inputs — deduplicated.
fn corpus() -> Vec<String> {
    let mut v = Vec::new();
    for base in BASES {
        for off in 0..=base.len() {
            if base.is_char_boundary(off) {
                v.push(format!("{}\u{e9}{}", &base[..off], &base[off..]));
                v.push(format!("{}\u{4e2d}{}", &base[..off], &base[off..]));
            }
            v.push(base[..off].to_string());
        }
    }
    for named in NAMED {
        v.push((*named).to_string());
    }
    v.sort();
    v.dedup();
    v
}

/// **N22.** Nothing in the corpus aborts, bare or as the type half of one
/// named argument. 103 of the 1,073 abort before the change, at 16 distinct
/// sites, which is the case for making the parser total.
///
/// No panic hook is installed — a hook is process-global and two cases running
/// in parallel would fight over it — so a failing run prints a panic line per
/// aborting input beside the assertion failure.
#[test]
fn n22_no_corpus_member_aborts_the_caller() {
    let corpus = corpus();
    let mut aborted: Vec<String> = Vec::new();
    for input in &corpus {
        if outcome(input).is_err() {
            aborted.push(format!("{input:?}"));
        }
        let wrapped = format!("Tuple(a {input})");
        if outcome(&wrapped).is_err() {
            aborted.push(format!("{wrapped:?}"));
        }
    }
    assert!(
        aborted.is_empty(),
        "{} of {} corpus inputs aborted rather than returning: {}",
        aborted.len(),
        corpus.len(),
        aborted.join(", ")
    );
}

/// **N23.** One input per changed index the corpus reaches other than the
/// `JSON` one, which is N17's. Each returns **an error** — neither a value nor
/// an abort, which is the whole of what the patch claims.
///
/// This is the case N22 cannot be: N22's list is of inputs that aborted, so a
/// site that stops aborting and starts returning a **value** leaves it green.
/// The two timezone inputs carry the message as well, because that site is the
/// one whose slice feeds an `Option`: dropping the timezone would answer
/// `DateTime64(3)`, a type the server never sent.
#[test]
fn n23_every_checked_index_returns_an_error() {
    for (input, site) in [
        ("FixedString(16)\u{e9}", ":505"),
        ("Array(UInt8)\u{e9}", ":525"),
        ("Enum16('a' = 1, 'b' = 2)\u{e9}", ":545"),
        ("DateTime('UTC')\u{4e2d}", ":559"),
        ("Decimal(9, 2)\u{e9}", ":569"),
        ("DateTime64(3)\u{e9}", ":606"),
        ("Time64(3)\u{e9}", ":624"),
        ("LowCardinality(String)\u{e9}", ":639"),
        ("SimpleAggregateFunction(", ":655"),
        ("Nullable(String)\u{e9}", ":685"),
        ("Map(String, UInt8)\u{e9}", ":696"),
        ("Tuple(UInt8, String)\u{e9}", ":750"),
        ("Variant(UInt8, String)\u{e9}", ":766"),
        ("Enum16('a' = 1, \u{e9}'b' = 2)", ":881"),
    ] {
        errs_at_all_without_aborting(input, site);
    }

    for input in ["DateTime64(3, \u{e9}'UTC')", "DateTime64(3, 'U"] {
        errs_without_aborting(
            input,
            "Invalid DateTime format, expected DateTime('timezone'), got ",
        );
    }
}
