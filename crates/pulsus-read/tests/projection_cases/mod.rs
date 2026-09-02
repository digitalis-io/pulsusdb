// Issue #479 — the matched-span projection differential's CASE REGISTRY,
// as typed data shared by the two checks that must not drift apart.
//
// `crates/pulsus-read/tests/traces_search_projection_differential.rs`
// runs these cases against the reference; the library test
// `pulsus_read::traces::search_plan::tests::the_live_differential_registry_covers_every_projection_shape`
// asserts the registry covers every projection shape the planner can
// produce. The library test used to read this array as TEXT, so a
// witness query appearing anywhere in the array — in a case NAME, for
// instance — satisfied the coverage check while exercising nothing
// (code review wave 2). Both sides now bind the same `const CASES`:
// the library test `include!`s this file, the differential `mod`s it,
// and coverage is decided by comparing `ProjectionCase::q` values.
//
// THE REQUIRED SET IS NOT IN THIS FILE, on purpose. It is
// `REQUIRED_SHAPE_PAIRS` in `crates/pulsus-read/src/traces/search_plan.rs`,
// which nothing here can edit — a registry cannot report its own
// absences. Code review wave 3 removed the mandated same-field
// physical-column case (34) and both registry guards stayed green,
// because the demand was then one witness per LABEL and both of that
// case's labels were supplied by other cases. Removing or rewriting any
// case that carries a required pair's witness query now fails that test,
// naming the pair.
//
// Not a top-level `tests/*.rs` file on purpose: Cargo would build it as
// its own integration-test target, which would select no tests and exit
// green.

/// One differential case: the query, and whether its reference value is a
/// `stringValue` (so the VALUES are compared too, not only the keys).
pub struct ProjectionCase {
    pub name: &'static str,
    pub q: &'static str,
    pub compare_values: bool,
}

const fn c(name: &'static str, q: &'static str) -> ProjectionCase {
    ProjectionCase {
        name,
        q,
        compare_values: true,
    }
}

const fn keys_only(name: &'static str, q: &'static str) -> ProjectionCase {
    ProjectionCase {
        name,
        q,
        compare_values: false,
    }
}

/// The registry — one entry per numbered row of the issue's query table.
/// The length is asserted at compile time so the count cannot drift from
/// the table it describes.
pub const CASES: [ProjectionCase; 35] = [
    c("01_name_eq", r#"{ name = "GET /pay" }"#),
    c("02_duration_gt", "{ duration > 1s }"),
    c("03_match_all", "{}"),
    c(
        "04_service_name",
        r#"{ resource.service.name = "proj-checkout" }"#,
    ),
    c("05_status_error", "{ status = error }"),
    c("06_span_http_method", r#"{ span.http.method = "GET" }"#),
    c(
        "07_disjunction_per_span",
        r#"{ name = "slow-op" || span.http.method = "GET" }"#,
    ),
    c(
        "08_scope_collision",
        r#"{ span.foo = "S-span" && resource.foo = "R-resource" }"#,
    ),
    c("09_unscoped_foo", r#"{ .foo = "S-span" }"#),
    keys_only("10_status_code_num", "{ span.http.status_code >= 500 }"),
    c("11_method_regex", r#"{ span.http.method =~ "GE.*" }"#),
    c("12_empty_value", r#"{ span.note = "" }"#),
    c("13_non_ascii_value", r#"{ span.city = "München" }"#),
    c("14_select_attr", "{} | select(span.http.method)"),
    c("15_select_name", "{} | select(name)"),
    c("16_select_duration", "{} | select(duration)"),
    c(
        "17_condition_and_select_same_field",
        r#"{ span.http.method = "GET" } | select(span.http.method)"#,
    ),
    c("18_status_message", r#"{ statusMessage = "boom" }"#),
    c("19_kind_client", "{ kind = client }"),
    c("20_parent_id", r#"{ span:parentID = "a479000000000001" }"#),
    c(
        "21_instrumentation_name",
        r#"{ instrumentation:name = "proj-scope" }"#,
    ),
    c(
        "22_event_scoped_attr",
        r#"{ event.exception.type = "IOError" }"#,
    ),
    c("23_event_name", r#"{ event:name = "exception" }"#),
    c("24_link_span_id", r#"{ link:spanID = "0a1b2c3d4e5f6071" }"#),
    keys_only("25_nested_set_left", "{ nestedSetLeft > 0 }"),
    c("26_trace_duration", "{ traceDuration > 1s }"),
    keys_only(
        "27_single_field_arithmetic",
        "{ span.duration_ms * 1000 > 5000 }",
    ),
    c("28_key_existence", "{ span.http.method != nil }"),
    c("29_root_name", r#"{ rootName = "GET /pay" }"#),
    c("30_name_neq_empty", r#"{ name != "" }"#),
    // 31-34: the SAME-FIELD comparison, one per value source it can draw
    // from — an attribute, an intrinsic that fills `name`, a nested-set
    // intrinsic and a physical column. Exactly one distinct field appears
    // across both operands, so each is a single-field condition and the
    // reference projects that field; a comparison naming two DIFFERENT
    // fields is the deferred multi-field class and is not a case here.
    c(
        "31_same_field_attribute",
        r#"{ span.http.method = span.http.method }"#,
    ),
    c("32_same_field_intrinsic_name", "{ name = name }"),
    keys_only(
        "33_same_field_nested_set",
        "{ nestedSetLeft = nestedSetLeft }",
    ),
    c(
        "34_same_field_physical_column",
        r#"{ resource.service.name = resource.service.name }"#,
    ),
    // The remaining uncovered value source: the scope VERSION column.
    c(
        "35_instrumentation_version",
        r#"{ instrumentation:version = "1.2.3" }"#,
    ),
];

const _: () = assert!(CASES.len() == 35);
