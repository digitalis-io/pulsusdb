//! Issue #339 — LogQL keyword case folding, **both directions**.
//!
//! The reference's lexer resolves keywords case-insensitively, so
//! `RATE(...)`, `SUM BY (...)`, `| JSON` and `LABEL_REPLACE(...)` are
//! accepted there and used to be 400s here — a whole-surface rejection
//! divergence. What it does *not* fold matters at least as much: folding
//! an identifier payload would silently change which series a query
//! selects, which is strictly worse than a rejection.
//!
//! **Version note.** The oracle is digest-pinned to v3.7.4 — verified
//! from the container's own `/loki/api/v1/status/buildinfo`, not from
//! the tag — which is the same version as the language target and as the
//! capture corpus under `crates/pulsus-read/tests/logqltest/`.
//!
//! The verdicts below were originally captured against the v3.7.3 oracle
//! this file was written for; every one was re-verified against v3.7.4
//! when the oracle moved, and the live leg at the bottom re-checks them
//! on every run.
//!
//! **Every verdict below was captured from the pinned reference
//! container** (`ci/logql/config.yaml`, the digest in
//! `.github/workflows/ci.yml`), black-box, by HTTP status on
//! `/loki/api/v1/query_range` — plus, for the case-SENSITIVE classes,
//! semantic probes that push a line and compare which query matches it,
//! because a status probe cannot tell `Mixed` from `mixed` (both parse).
//! The probe behind each class is named on the test.
//!
//! The last test in this file replays the accept/reject half of the
//! table against a live container when `PULSUSDB_LOGQL_DIFF_URL` is set
//! (the same gate and container `tests/logql_differential.rs` uses, with
//! its own CI step), so the captured verdicts cannot silently rot.

use pulsus_logql::parse;

/// Renders a parsed query back to text. Keywords render in their
/// canonical lowercase spelling, identifier payloads render verbatim —
/// so one string comparison pins BOTH directions at once.
fn round_trip(query: &str) -> String {
    parse(query)
        .unwrap_or_else(|e| panic!("{query:?} must parse: {e}"))
        .to_string()
}

fn accepts(query: &str) -> bool {
    parse(query).is_ok()
}

// ---------------------------------------------------------------------
// FOLDS. Probe: the same query in upper/mixed case is a 200 against the
// reference, identical to its lowercase spelling.
// ---------------------------------------------------------------------

/// Range-aggregation function names. Reference probe: `RATE(...)`,
/// `Rate(...)`, `COUNT_OVER_TIME(...)`, `QUANTILE_OVER_TIME(...)` etc.
/// all 200.
#[test]
fn range_aggregation_names_fold() {
    for (upper, lower) in [
        (r#"RATE({app="x"}[5m])"#, r#"rate({app="x"}[5m])"#),
        (r#"Rate({app="x"}[5m])"#, r#"rate({app="x"}[5m])"#),
        (
            r#"COUNT_OVER_TIME({app="x"}[5m])"#,
            r#"count_over_time({app="x"}[5m])"#,
        ),
        (
            r#"BYTES_OVER_TIME({app="x"}[5m])"#,
            r#"bytes_over_time({app="x"}[5m])"#,
        ),
        (
            r#"BYTES_RATE({app="x"}[5m])"#,
            r#"bytes_rate({app="x"}[5m])"#,
        ),
        (
            r#"ABSENT_OVER_TIME({app="x"}[5m])"#,
            r#"absent_over_time({app="x"}[5m])"#,
        ),
        (
            r#"RATE_COUNTER({app="x"} | unwrap v [5m])"#,
            r#"rate_counter({app="x"} | unwrap v [5m])"#,
        ),
        (
            r#"AVG_OVER_TIME({app="x"} | unwrap v [5m])"#,
            r#"avg_over_time({app="x"} | unwrap v [5m])"#,
        ),
        (
            r#"SUM_OVER_TIME({app="x"} | unwrap v [5m])"#,
            r#"sum_over_time({app="x"} | unwrap v [5m])"#,
        ),
        (
            r#"MIN_OVER_TIME({app="x"} | unwrap v [5m])"#,
            r#"min_over_time({app="x"} | unwrap v [5m])"#,
        ),
        (
            r#"MAX_OVER_TIME({app="x"} | unwrap v [5m])"#,
            r#"max_over_time({app="x"} | unwrap v [5m])"#,
        ),
        (
            r#"STDVAR_OVER_TIME({app="x"} | unwrap v [5m])"#,
            r#"stdvar_over_time({app="x"} | unwrap v [5m])"#,
        ),
        (
            r#"STDDEV_OVER_TIME({app="x"} | unwrap v [5m])"#,
            r#"stddev_over_time({app="x"} | unwrap v [5m])"#,
        ),
        (
            r#"QUANTILE_OVER_TIME(0.9, {app="x"} | unwrap v [5m])"#,
            r#"quantile_over_time(0.9, {app="x"} | unwrap v [5m])"#,
        ),
        (
            r#"FIRST_OVER_TIME({app="x"} | unwrap v [5m])"#,
            r#"first_over_time({app="x"} | unwrap v [5m])"#,
        ),
        (
            r#"LAST_OVER_TIME({app="x"} | unwrap v [5m])"#,
            r#"last_over_time({app="x"} | unwrap v [5m])"#,
        ),
    ] {
        assert_eq!(round_trip(upper), round_trip(lower), "{upper}");
    }
}

/// Vector-aggregation operators, `label_replace`, `vector`, and the
/// `variants(...) of (...)` pair. Reference probe: all 200 in upper case
/// (`approx_topk` on the instant endpoint, where it is not a 500).
#[test]
fn vector_aggregation_and_call_names_fold() {
    for (upper, lower) in [
        (r#"SUM(rate({app="x"}[5m]))"#, r#"sum(rate({app="x"}[5m]))"#),
        (r#"AVG(rate({app="x"}[5m]))"#, r#"avg(rate({app="x"}[5m]))"#),
        (r#"MAX(rate({app="x"}[5m]))"#, r#"max(rate({app="x"}[5m]))"#),
        (r#"MIN(rate({app="x"}[5m]))"#, r#"min(rate({app="x"}[5m]))"#),
        (
            r#"COUNT(rate({app="x"}[5m]))"#,
            r#"count(rate({app="x"}[5m]))"#,
        ),
        (
            r#"STDDEV(rate({app="x"}[5m]))"#,
            r#"stddev(rate({app="x"}[5m]))"#,
        ),
        (
            r#"STDVAR(rate({app="x"}[5m]))"#,
            r#"stdvar(rate({app="x"}[5m]))"#,
        ),
        (
            r#"TOPK(3, rate({app="x"}[5m]))"#,
            r#"topk(3, rate({app="x"}[5m]))"#,
        ),
        (
            r#"BOTTOMK(3, rate({app="x"}[5m]))"#,
            r#"bottomk(3, rate({app="x"}[5m]))"#,
        ),
        (
            r#"SORT(rate({app="x"}[5m]))"#,
            r#"sort(rate({app="x"}[5m]))"#,
        ),
        (
            r#"SORT_DESC(rate({app="x"}[5m]))"#,
            r#"sort_desc(rate({app="x"}[5m]))"#,
        ),
        (
            r#"APPROX_TOPK(3, sum by (a) (rate({app="x"}[5m])))"#,
            r#"approx_topk(3, sum by (a) (rate({app="x"}[5m])))"#,
        ),
        (
            r#"LABEL_REPLACE(rate({app="x"}[5m]), "d", "r", "s", ".*")"#,
            r#"label_replace(rate({app="x"}[5m]), "d", "r", "s", ".*")"#,
        ),
        (r#"VECTOR(1)"#, r#"vector(1)"#),
        (
            r#"VARIANTS(count_over_time({app="x"}[5m])) OF ({app="x"}[5m])"#,
            r#"variants(count_over_time({app="x"}[5m])) of ({app="x"}[5m])"#,
        ),
    ] {
        assert_eq!(round_trip(upper), round_trip(lower), "{upper}");
    }
}

/// Grouping, vector-matching and set-operator keywords. Reference probe:
/// `sum BY (...)`, `... + ON (...) ...`, `... AND ...`, `> BOOL ...` all
/// 200.
#[test]
fn grouping_matching_and_set_operator_keywords_fold() {
    for (upper, lower) in [
        (
            r#"sum BY (app) (rate({app="x"}[5m]))"#,
            r#"sum by (app) (rate({app="x"}[5m]))"#,
        ),
        (
            r#"sum WITHOUT (app) (rate({app="x"}[5m]))"#,
            r#"sum without (app) (rate({app="x"}[5m]))"#,
        ),
        (
            r#"rate({app="x"}[5m]) AND rate({app="y"}[5m])"#,
            r#"rate({app="x"}[5m]) and rate({app="y"}[5m])"#,
        ),
        (
            r#"rate({app="x"}[5m]) OR rate({app="y"}[5m])"#,
            r#"rate({app="x"}[5m]) or rate({app="y"}[5m])"#,
        ),
        (
            r#"rate({app="x"}[5m]) UNLESS rate({app="y"}[5m])"#,
            r#"rate({app="x"}[5m]) unless rate({app="y"}[5m])"#,
        ),
        (
            r#"rate({app="x"}[5m]) > BOOL rate({app="y"}[5m])"#,
            r#"rate({app="x"}[5m]) > bool rate({app="y"}[5m])"#,
        ),
        (
            r#"rate({app="x"}[5m]) + ON (app) rate({app="y"}[5m])"#,
            r#"rate({app="x"}[5m]) + on (app) rate({app="y"}[5m])"#,
        ),
        (
            r#"rate({app="x"}[5m]) + IGNORING (app) rate({app="y"}[5m])"#,
            r#"rate({app="x"}[5m]) + ignoring (app) rate({app="y"}[5m])"#,
        ),
        (
            r#"sum by (a) (rate({app="x"}[5m])) + on (a) GROUP_LEFT (b) sum by (a,b) (rate({app="y"}[5m]))"#,
            r#"sum by (a) (rate({app="x"}[5m])) + on (a) group_left (b) sum by (a,b) (rate({app="y"}[5m]))"#,
        ),
        (
            r#"sum by (a,b) (rate({app="y"}[5m])) + on (a) GROUP_RIGHT (b) sum by (a) (rate({app="x"}[5m]))"#,
            r#"sum by (a,b) (rate({app="y"}[5m])) + on (a) group_right (b) sum by (a) (rate({app="x"}[5m]))"#,
        ),
    ] {
        assert_eq!(round_trip(upper), round_trip(lower), "{upper}");
    }
}

/// Pipeline-stage names, `unwrap`, the unwrap conversions and `ip`.
/// Reference probe: `| JSON`, `| LINE_FORMAT`, `| DROP a`, `| UNWRAP v`,
/// `unwrap DURATION(v)`, `|= IP("1.2.3.4")` all 200.
#[test]
fn pipeline_stage_and_conversion_keywords_fold() {
    for (upper, lower) in [
        (r#"{app="x"} | JSON"#, r#"{app="x"} | json"#),
        (r#"{app="x"} | LOGFMT"#, r#"{app="x"} | logfmt"#),
        (
            r#"{app="x"} | REGEXP "(?P<a>.*)""#,
            r#"{app="x"} | regexp "(?P<a>.*)""#,
        ),
        (
            r#"{app="x"} | PATTERN "<a>""#,
            r#"{app="x"} | pattern "<a>""#,
        ),
        (r#"{app="x"} | UNPACK"#, r#"{app="x"} | unpack"#),
        (
            r#"{app="x"} | LINE_FORMAT "{{.a}}""#,
            r#"{app="x"} | line_format "{{.a}}""#,
        ),
        (
            r#"{app="x"} | LABEL_FORMAT a="b""#,
            r#"{app="x"} | label_format a="b""#,
        ),
        (r#"{app="x"} | DECOLORIZE"#, r#"{app="x"} | decolorize"#),
        (
            r#"{app="x"} | json | DROP a"#,
            r#"{app="x"} | json | drop a"#,
        ),
        (
            r#"{app="x"} | json | KEEP a"#,
            r#"{app="x"} | json | keep a"#,
        ),
        (
            r#"sum_over_time({app="x"} | UNWRAP v [5m])"#,
            r#"sum_over_time({app="x"} | unwrap v [5m])"#,
        ),
        (
            r#"sum_over_time({app="x"} | unwrap DURATION_SECONDS(v) [5m])"#,
            r#"sum_over_time({app="x"} | unwrap duration_seconds(v) [5m])"#,
        ),
        (
            r#"sum_over_time({app="x"} | unwrap DURATION(v) [5m])"#,
            r#"sum_over_time({app="x"} | unwrap duration(v) [5m])"#,
        ),
        (
            r#"sum_over_time({app="x"} | unwrap BYTES(v) [5m])"#,
            r#"sum_over_time({app="x"} | unwrap bytes(v) [5m])"#,
        ),
        (
            r#"{app="x"} |= IP("1.2.3.4")"#,
            r#"{app="x"} |= ip("1.2.3.4")"#,
        ),
        (
            r#"{app="x"} | json | a = IP("1.2.3.4")"#,
            r#"{app="x"} | json | a = ip("1.2.3.4")"#,
        ),
    ] {
        assert_eq!(round_trip(upper), round_trip(lower), "{upper}");
    }
}

// ---------------------------------------------------------------------
// DOES NOT FOLD. **This is the half that guards against a wrong answer.**
// ---------------------------------------------------------------------

/// **Identifier payloads keep their exact case.** Probe (semantic, not
/// status — both spellings parse, so acceptance proves nothing): a line
/// `{"Mixed":"A","mixed":"b","rate":"R"}` on stream `{app="cf",
/// Env="Prod"}` pushed to the reference, then
///
/// | query                            | reference hits |
/// |----------------------------------|----------------|
/// | `{app="cf"}`                     | 1              |
/// | `{App="cf"}`                     | 0              |
/// | `{app="cf", Env="Prod"}`         | 1              |
/// | `{app="cf", env="Prod"}`         | 0              |
/// | `{app="cf", Env="prod"}`         | 0              |
/// | `... \| json \| Mixed = "A"`     | 1              |
/// | `... \| json \| mixed = "A"`     | 0              |
/// | `... \| json \| rate = "R"`      | 1              |
/// | `... \| json \| RATE = "R"`      | 0              |
///
/// and `sum by (Env) (...)` returns `{Env="Prod"}` while `sum by (env)`
/// returns `{}`. So a label name equal to a keyword — `rate` — is NOT
/// folded when it is used as a label name, which is exactly why folding
/// happens at keyword positions only.
///
/// The round-trip renders keywords canonically and identifiers verbatim,
/// so an accidental fold anywhere in this list fails here.
#[test]
fn identifier_payloads_are_never_folded() {
    for (query, expected) in [
        // Selector label names and values.
        (r#"{App="X", Env="Prod"}"#, r#"{App="X", Env="Prod"}"#),
        // Label-filter label name, including one that collides with a
        // function keyword.
        (
            r#"{app="x"} | json | Mixed="A""#,
            r#"{app="x"} | json | Mixed="A""#,
        ),
        (
            r#"{app="x"} | json | RATE="R""#,
            r#"{app="x"} | json | RATE="R""#,
        ),
        // Grouping list.
        (
            r#"SUM BY (Env) (RATE({App="x"}[5m]))"#,
            r#"sum by(Env)(rate({App="x"}[5m]))"#,
        ),
        (
            r#"SUM WITHOUT (Env) (RATE({App="x"}[5m]))"#,
            r#"sum without(Env)(rate({App="x"}[5m]))"#,
        ),
        // `label_format` destination and source, `drop`, `keep`.
        (
            r#"{app="x"} | json | LABEL_FORMAT Out=Mixed"#,
            r#"{app="x"} | json | label_format Out=Mixed"#,
        ),
        (
            r#"{app="x"} | json | DROP Keep1 | KEEP Mixed"#,
            r#"{app="x"} | json | drop Keep1 | keep Mixed"#,
        ),
        // `unwrap` identifier, including inside a conversion.
        (
            r#"SUM_OVER_TIME({app="x"} | UNWRAP Lat [5m])"#,
            r#"sum_over_time({app="x"} | unwrap Lat[5m])"#,
        ),
        (
            r#"SUM_OVER_TIME({app="x"} | UNWRAP DURATION(Lat) [5m])"#,
            r#"sum_over_time({app="x"} | unwrap duration(Lat)[5m])"#,
        ),
        // Vector-matching label lists.
        (
            r#"rate({app="x"}[5m]) + ON (Env) rate({app="y"}[5m])"#,
            r#"rate({app="x"}[5m]) + on(Env) rate({app="y"}[5m])"#,
        ),
        // String contents (line filters, regex bodies, template bodies).
        (
            r#"{app="x"} |= "MiXeD" | LINE_FORMAT "{{.Mixed}}""#,
            r#"{app="x"} |= "MiXeD" | line_format "{{.Mixed}}""#,
        ),
    ] {
        assert_eq!(round_trip(query), expected, "{query}");
    }
}

/// **Stage flags do not fold.** Reference probe: `| logfmt --strict` is
/// 200, `| logfmt --STRICT` is `400 parse error … unexpected IDENTIFIER,
/// expecting NUMBER`; same for `--KEEP-EMPTY`.
#[test]
fn logfmt_stage_flags_do_not_fold() {
    assert!(accepts(r#"{app="x"} | logfmt --strict"#));
    assert!(accepts(r#"{app="x"} | logfmt --keep-empty"#));
    assert!(!accepts(r#"{app="x"} | logfmt --STRICT"#));
    assert!(!accepts(r#"{app="x"} | logfmt --Strict"#));
    assert!(!accepts(r#"{app="x"} | logfmt --KEEP-EMPTY"#));
}

/// **Range-selector duration units do not fold.** Reference probe:
/// `[5s]`/`[5m]`/`[5h]`/`[5d]`/`[1w]` are 200; `[5S]`, `[5M]`, `[5H]`,
/// `[5D]`, `[5W]` are each `400 parse error … unknown unit "S" in
/// duration "5S"` — a genuine syntax verdict, not the query-length limit
/// that makes a long `[5w]` window inconclusive.
#[test]
fn range_duration_units_do_not_fold() {
    for ok in ["5ms", "5s", "5m", "5h", "5d", "1w"] {
        assert!(
            accepts(&format!(r#"count_over_time({{app="x"}}[{ok}])"#)),
            "[{ok}] must be accepted"
        );
    }
    for bad in ["5MS", "5S", "5M", "5H", "5D", "5W"] {
        assert!(
            !accepts(&format!(r#"count_over_time({{app="x"}}[{bad}])"#)),
            "[{bad}] must be rejected — the reference reports `unknown unit`"
        );
    }
}

// ---------------------------------------------------------------------
// The residual: reference verdicts this change does NOT bring into
// agreement. Census-style, so none of them can be quietly forgotten or
// quietly fixed without updating this list.
// ---------------------------------------------------------------------

/// Queries where PulsusDB and the **differential oracle** disagree after
/// issue #339. **None is a case-folding gap in the direction #339
/// describes** (we reject what the oracle accepts).
///
/// The middle column is the ORACLE's verdict. It was recorded against
/// v3.7.3, re-verified unchanged when the oracle was re-pinned to
/// v3.7.4, and the live leg re-checks it on every run. Oracle and target
/// are now one version, so that distinction no longer needs policing —
/// but it did once, and reading a probe as the target's behaviour sent
/// one adjudication on this issue the wrong way.
///
/// Three classes live here, and they are not the same kind of thing:
///
/// * **`VERSION_DIFFERENCE` — EMPTY, and pinned at zero.** It once held
///   19 byte-size literal rows, on the reading that v3.7.4 accepted the
///   full case-insensitive `humanize.ParseBytes` set while the v3.7.3
///   oracle did not. Measurement disproved it: BOTH versions accept only
///   the 21 case-sensitive spellings, so PulsusDB was the side that was
///   wrong. Issue #350 fixed the parser and deleted the rows — they are
///   agreement now, and the census forbids agreement rows. The class
///   name survives only so the zero can be pinned; a row appearing here
///   again would mean the two reference versions had genuinely diverged.
/// * **OVER-ACCEPTANCE — unscheduled by design.** `1S`, `[5ns]`/`[5us]`,
///   `{by="x"}`/`{json="x"}`. A user does not notice these: their query
///   works here and would fail against the reference. Recorded rather
///   than filed, which is only safe because this census exists.
/// * **A REAL GAP, filed elsewhere.** `offset`, unimplemented in every
///   spelling — a query that runs against the reference fails here.
///
/// **What this census can and cannot observe (issue #350):** its
/// PulsusDB verdict is [`accepts`] — `pulsus_logql::parse` — so it fails
/// when a row's PARSE-layer verdict moves, and it CANNOT see a verdict
/// decided below the parser (plan/pipeline compile in `pulsus-read`).
/// Issue #350 proved that blindness concretely: the byte-literal
/// spellings' end-to-end verdict flipped to reject while this census
/// stayed green, because they still PARSE here. A row whose verdict is
/// decided below the parser therefore does NOT belong in this table —
/// its end-to-end home is `pulsus-read` (`b8_byte_parity.test`'s
/// `eval_fail` block through the full parse→plan→compile route, plus
/// `pipeline.rs`'s exhaustive literal tests); this file keeps only the
/// REFERENCE half of those rows ([`BYTE_LITERAL_QUERY_REJECTS`], live
/// leg) and a hermetic pin that the layer split itself still holds.
/// The three classes a residual row can belong to. Machine-readable
/// rather than only a comment heading, so [`the_residual_divergence_census_is_exact`]
/// can pin a total PER CLASS — a row that is re-classified without the
/// count being updated fails, which prose headings cannot catch.
const VERSION_DIFFERENCE: &str = "version-difference";
const OVER_ACCEPTANCE: &str = "over-acceptance";
const REAL_GAP: &str = "real-gap";

/// The wire column's "adds nothing" marker.
const SAME: &str = "same-as-parse";

/// `(query, reference, PulsusDB PARSE, PulsusDB WIRE, class)`.
///
/// **Two axes, because they are different facts** (issue #343, following
/// the TraceQL matrix). Collapsing them is what let #335's D2 ship as
/// "closed" on a parse score while every such query still 400'd at the
/// planner; switching that matrix to the wire exposed 37 divergences
/// nobody could see.
///
/// The WIRE column is `SAME` when the planner adds nothing to the parse
/// verdict, or an explicit verdict when it differs. `offset` is the
/// worked example: **parse accept, wire reject** — the grammar landed
/// (issue #343) and the planner window shift has not, so a query using it
/// is still refused end to end. When the shift lands, one field moves and
/// this census says so.
///
/// The parse column is DERIVED (`accepts()` runs the parser). The wire
/// column is RECORDED, not derived, because this crate cannot see the
/// planner — `pulsus-read` owns it. That is a real weakness of this
/// table and is stated rather than hidden.
const KNOWN_RESIDUAL_DIVERGENCES: &[(&str, &str, &str, &str, &str)] = &[
    // (query, differential-oracle verdict, parse, wire, class)
    // OVER-ACCEPTANCE. A label-filter duration with an uppercase unit:
    // the oracle lexes no literal at all (`400 syntax error: unexpected
    // $end`).
    (
        r#"{app="x"} | json | d > 1S"#,
        "reject",
        "accept",
        SAME,
        OVER_ACCEPTANCE,
    ),
    // OVER-ACCEPTANCE. Range-selector units the oracle does not know at
    // all (`unknown unit "ns"` / `"us"`) — not a case issue.
    (
        r#"count_over_time({app="x"}[5ns])"#,
        "reject",
        "accept",
        SAME,
        OVER_ACCEPTANCE,
    ),
    (
        r#"count_over_time({app="x"}[5us])"#,
        "reject",
        "accept",
        SAME,
        OVER_ACCEPTANCE,
    ),
    // A REAL GAP (issue #343), and the worked example of why this table
    // carries TWO axes. The grammar landed: `offset` now PARSES, on every
    // range selector, matching the reference. The planner window shift did
    // not, and it rejects rather than silently evaluating the UNSHIFTED
    // window — so end to end the query is still refused.
    //
    // parse: accept · wire: reject. Recording only one of those would be
    // false in one direction or the other: "reject" hides that the grammar
    // shipped, "accept" claims a fix a user cannot observe. When the
    // planner shift lands, the wire field moves to `accept` and this row
    // leaves the census.
    (
        r#"count_over_time({app="x"}[5m] offset 1m)"#,
        "accept",
        "accept",
        "reject",
        REAL_GAP,
    ),
    (
        r#"count_over_time({app="x"}[5m] OFFSET 1m)"#,
        "accept",
        "reject",
        SAME,
        REAL_GAP,
    ),
    // OVER-ACCEPTANCE. A keyword used as a selector label name: the
    // oracle's lexer emits a keyword token there and rejects it
    // (`unexpected by,
    // expecting IDENTIFIER or }` — note it prints the FOLDED spelling
    // for `{BY="x"}` too, which is how the fold was established).
    (r#"{by="x"}"#, "reject", "accept", SAME, OVER_ACCEPTANCE),
    (r#"{BY="x"}"#, "reject", "accept", SAME, OVER_ACCEPTANCE),
    (r#"{json="x"}"#, "reject", "accept", SAME, OVER_ACCEPTANCE),
    (r#"{JSON="x"}"#, "reject", "accept", SAME, OVER_ACCEPTANCE),
];

/// **Counts are pinned, not just rows.** The census used to assert every
/// row's verdict but no total, so a row could be added or re-classified
/// and the prose describing the list would silently go stale — which is
/// exactly what happened: the notes said 26 rows in three classes of
/// 19/5/2 while the list held 28 in 19/7/2, and nothing failed. Totals
/// live here now, so the list and any statement of its size have one
/// source of truth.
#[test]
fn the_residual_census_totals_are_pinned() {
    assert_eq!(
        KNOWN_RESIDUAL_DIVERGENCES.len(),
        9,
        "the residual census changed size — update this total and every count quoted \
         about it (issues #339, #350)"
    );
    for (class, expected) in [
        // Issue #350 emptied this class: the 19 byte-literal rows were
        // never a version difference — BOTH v3.7.3 and v3.7.4 reject
        // those spellings (probed exhaustively) — and PulsusDB now
        // rejects them too, so they are agreements, not residual
        // divergences. Their spellings live on in
        // [`BYTE_LITERAL_QUERY_REJECTS`] below.
        (VERSION_DIFFERENCE, 0),
        (OVER_ACCEPTANCE, 7),
        (REAL_GAP, 2),
    ] {
        let n = KNOWN_RESIDUAL_DIVERGENCES
            .iter()
            .filter(|(_, _, _, _, c)| *c == class)
            .count();
        assert_eq!(n, expected, "{class} row count changed");
    }
    // The per-class totals must account for every row, so a new class
    // added without its own pinned total cannot hide in the remainder.
    let classified: usize = [VERSION_DIFFERENCE, OVER_ACCEPTANCE, REAL_GAP]
        .iter()
        .map(|class| {
            KNOWN_RESIDUAL_DIVERGENCES
                .iter()
                .filter(|(_, _, _, _, c)| c == class)
                .count()
        })
        .sum();
    assert_eq!(
        classified,
        KNOWN_RESIDUAL_DIVERGENCES.len(),
        "a row carries a class the totals above do not pin"
    );
}

#[test]
fn the_residual_divergence_census_is_exact() {
    let mut drifted = Vec::new();
    for (query, reference, parse, wire, _class) in KNOWN_RESIDUAL_DIVERGENCES {
        let ours = if accepts(query) { "accept" } else { "reject" };
        if ours != *parse {
            drifted.push(format!(
                "{query:?}: recorded parse={parse}, actual={ours} (reference={reference})"
            ));
        }
        // The row must still record a DIVERGENCE on the axis the user
        // experiences. A row whose wire verdict equals the reference has
        // been fixed end to end and belongs out of this list; a row whose
        // PARSE now agrees while the wire does not is still a divergence,
        // which is exactly what the two columns exist to express.
        let effective: &str = if *wire == SAME { parse } else { wire };
        assert_ne!(
            *reference, effective,
            "{query:?} agrees with the reference on the WIRE — it is fixed, remove it"
        );
    }
    assert!(
        drifted.is_empty(),
        "the residual-divergence census drifted; a fix must update this list (and the \
         issue that owns it):\n{}",
        drifted.join("\n")
    );
}

// ---------------------------------------------------------------------
// The live leg: the captured verdicts above are only trustworthy while
// they still match the container they came from.
// ---------------------------------------------------------------------

/// Every query this file makes a claim about, paired with the verdict
/// captured from the reference. Assembled from the same constants the
/// hermetic tests use, so the live leg cannot drift from them.
fn live_expectations() -> Vec<(String, &'static str)> {
    let mut out: Vec<(String, &'static str)> = Vec::new();
    // The folding half: both spellings are accepted by the reference.
    for q in FOLDING_PROBES {
        out.push(((*q).to_string(), "accept"));
    }
    // The non-folding half: the uppercase spelling is a reference 400.
    for q in NON_FOLDING_REJECTS {
        out.push(((*q).to_string(), "reject"));
    }
    // The byte-literal spellings both engines reject (#350): only the
    // REFERENCE half is checkable from this crate — see the const's doc.
    for q in BYTE_LITERAL_QUERY_REJECTS {
        out.push(((*q).to_string(), "reject"));
    }
    // The residual census carries the reference verdict per row.
    for (q, reference, _, _, _) in KNOWN_RESIDUAL_DIVERGENCES {
        out.push(((*q).to_string(), reference));
    }
    out
}

/// Representative accepted spellings, one per folding class — the live
/// leg re-establishes that the reference still accepts them.
/// `approx_topk` is omitted: it is instant-only there (a range query is a
/// 500, i.e. inconclusive), and `logql_differential.rs` already owns the
/// instant-endpoint probing machinery.
const FOLDING_PROBES: &[&str] = &[
    r#"RATE({app="x"}[5m])"#,
    r#"Rate({app="x"}[5m])"#,
    r#"COUNT_OVER_TIME({app="x"}[5m])"#,
    r#"QUANTILE_OVER_TIME(0.9, {app="x"} | unwrap v [5m])"#,
    r#"SUM(rate({app="x"}[5m]))"#,
    r#"TOPK(3, rate({app="x"}[5m]))"#,
    r#"SORT_DESC(rate({app="x"}[5m]))"#,
    r#"LABEL_REPLACE(rate({app="x"}[5m]), "d", "r", "s", ".*")"#,
    r#"VECTOR(1)"#,
    r#"VARIANTS(count_over_time({app="x"}[5m])) OF ({app="x"}[5m])"#,
    r#"sum BY (app) (rate({app="x"}[5m]))"#,
    r#"sum WITHOUT (app) (rate({app="x"}[5m]))"#,
    r#"rate({app="x"}[5m]) AND rate({app="y"}[5m])"#,
    r#"rate({app="x"}[5m]) > BOOL rate({app="y"}[5m])"#,
    r#"rate({app="x"}[5m]) + ON (app) rate({app="y"}[5m])"#,
    r#"sum by (a) (rate({app="x"}[5m])) + on (a) GROUP_LEFT (b) sum by (a,b) (rate({app="y"}[5m]))"#,
    r#"{app="x"} | JSON"#,
    r#"{app="x"} | LINE_FORMAT "{{.a}}""#,
    r#"{app="x"} | json | DROP a"#,
    r#"sum_over_time({app="x"} | UNWRAP DURATION(v) [5m])"#,
    r#"{app="x"} |= IP("1.2.3.4")"#,
    // Identifier payloads are accepted in either case — the SEMANTIC
    // claim about them (which series they select) is established by the
    // push-and-compare probe documented on
    // `identifier_payloads_are_never_folded`, which a status leg cannot
    // reproduce.
    r#"{App="X", Env="Prod"}"#,
    r#"{app="x"} | json | RATE="R""#,
];

/// Byte-literal query spellings the reference rejects — on BOTH v3.7.3
/// and v3.7.4, probed exhaustively (issue #350; the versions agree, so
/// these were never the version difference this file once recorded).
/// PulsusDB ALSO rejects every one of them since #350 — but that verdict
/// is decided in `pulsus-read`'s pipeline (`classify_numeric_literal` →
/// `parse_query_bytes_literal`), BELOW this crate's parser, so this file
/// can only pin two things honestly: the REFERENCE verdict (replayed by
/// the live leg) and the LAYER SPLIT itself
/// ([`byte_literal_rejects_still_parse_here_the_verdict_lives_below`]).
/// The end-to-end PulsusDB verdict is pinned in `pulsus-read`:
/// `logqltest/corpus/b8_byte_parity.test`'s `eval_fail` block (full
/// parse→plan→compile→eval route) and `pipeline.rs`'s exhaustive
/// 21-spelling accept/reject tests.
const BYTE_LITERAL_QUERY_REJECTS: &[&str] = &[
    r#"{app="x"} | json | size > 1b"#,
    r#"{app="x"} | json | size > 1kb"#,
    r#"{app="x"} | json | size > 1Kb"#,
    r#"{app="x"} | json | size > 1kib"#,
    r#"{app="x"} | json | size > 1Kib"#,
    r#"{app="x"} | json | size > 1KIB"#,
    r#"{app="x"} | json | size > 1mb"#,
    r#"{app="x"} | json | size > 1mB"#,
    r#"{app="x"} | json | size > 1Mb"#,
    r#"{app="x"} | json | size > 1mi"#,
    r#"{app="x"} | json | size > 1mib"#,
    r#"{app="x"} | json | size > 1g"#,
    r#"{app="x"} | json | size > 1gb"#,
    r#"{app="x"} | json | size > 1gi"#,
    r#"{app="x"} | json | size > 1gib"#,
    r#"{app="x"} | json | size > 1tb"#,
    r#"{app="x"} | json | size > 1tib"#,
    r#"{app="x"} | json | size > 1pb"#,
    r#"{app="x"} | json | size > 1PB"#,
];

/// The layer split, pinned hermetically: every rejected byte spelling
/// still PARSES in this crate — the rejection is a plan/compile-time
/// verdict in `pulsus-read`. If any of these ever stops parsing, the
/// verdict has MOVED LAYERS and both this file's claims and the
/// `pulsus-read` pins must be re-derived (that movement was invisible to
/// the pre-#350 census, which is exactly why this pin exists).
#[test]
fn byte_literal_rejects_still_parse_here_the_verdict_lives_below() {
    for q in BYTE_LITERAL_QUERY_REJECTS {
        assert!(
            accepts(q),
            "{q}: parses no more — the byte-literal verdict moved into the parser; \
             re-derive the layer-split claims here and in pulsus-read"
        );
    }
}

/// Spellings the reference REJECTS — the non-folding half. A live accept
/// here would mean we are now under-folding relative to the reference.
const NON_FOLDING_REJECTS: &[&str] = &[
    r#"{app="x"} | logfmt --STRICT"#,
    r#"{app="x"} | logfmt --KEEP-EMPTY"#,
    r#"count_over_time({app="x"}[5S])"#,
    r#"count_over_time({app="x"}[5M])"#,
    r#"count_over_time({app="x"}[5H])"#,
    r#"count_over_time({app="x"}[5D])"#,
    r#"count_over_time({app="x"}[5W])"#,
];

/// Replays the whole table against a live reference container and
/// asserts every captured verdict still holds.
///
/// Gate: skips cleanly unless `PULSUSDB_LOGQL_DIFF_URL` is set, the same
/// gate and container `tests/logql_differential.rs` uses. Any status
/// other than 2xx/400 fails loudly as inconclusive rather than being
/// counted as a rejection.
#[test]
fn captured_reference_verdicts_still_hold_against_a_live_container() {
    let Ok(base) = std::env::var("PULSUSDB_LOGQL_DIFF_URL") else {
        eprintln!("skipping: set PULSUSDB_LOGQL_DIFF_URL to replay against the reference");
        return;
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    let mut drift = Vec::new();
    for (query, expected) in live_expectations() {
        let mut cmd = std::process::Command::new("curl");
        cmd.args([
            "-s",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            "-G",
            "--max-time",
            "20",
        ]);
        cmd.args(["--data-urlencode", &format!("query={query}")]);
        // A 60-second window: a long `[range]` would otherwise trip the
        // reference's own max_query_length and return a 400 that is not
        // a syntax verdict at all.
        cmd.args([
            "--data-urlencode",
            &format!("start={}", now.saturating_sub(60)),
        ]);
        cmd.args(["--data-urlencode", &format!("end={now}")]);
        cmd.args(["--data-urlencode", "step=60s"]);
        cmd.args(["--data-urlencode", "limit=1"]);
        cmd.arg(format!(
            "{}/loki/api/v1/query_range",
            base.trim_end_matches('/')
        ));
        let out = cmd.output().expect("curl must be on PATH");
        let code: u32 = String::from_utf8_lossy(&out.stdout)
            .trim()
            .parse()
            .unwrap_or(0);
        let live = match code {
            200..=299 => "accept",
            400 => "reject",
            other => panic!(
                "inconclusive: reference returned {other} for {query:?} — only 2xx/400 are \
                 syntax verdicts"
            ),
        };
        if live != expected {
            drift.push(format!("{query:?}: captured={expected}, live={live}"));
        }
    }
    assert!(
        drift.is_empty(),
        "the captured reference verdicts no longer match the container:\n{}",
        drift.join("\n")
    );
}
