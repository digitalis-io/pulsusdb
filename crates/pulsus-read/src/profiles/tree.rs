//! Issue #823: `SelectMergeStacktraces` planned as one statement (S5) —
//! the merged tree of the selected samples, one row per node, computed in
//! ClickHouse. `super::flamegraph` trims the rows to the flame graph.
//!
//! The statement reads `profile_samples` once: the per-stack sums are the
//! join's build side, and the tenant's stack rows for the window's receive
//! days the probe side, deduplicated per `stack_id` after the join.

use pulsus_model::Tenant;
use serde::{Deserialize, Serialize};

use crate::logql::escape::ch_string;
use crate::metrics::{DataWindow, LabelMatcher};
use crate::traces::tenant_scope::TenantTerm;

use super::engine::{ProfilesReadConfig, ProfilesReadError};
use super::selector::{Compiled, SelectorError, SelectorRequest, compile};

/// The earliest a stack row's receive day can fall before the window: a
/// sample stamped `t` is admitted only when it was received no more than
/// 10 minutes before `t` (`profile_model::REJECT_NEWER_THAN_MS`).
pub const STACK_DAY_BEFORE_MS: i64 = 600_000;
/// The latest a stack row's receive day can fall after the window: a sample
/// is admitted only when it was received no more than 1 hour after its
/// time (`profile_model::REJECT_OLDER_THAN_MS`).
pub const STACK_DAY_AFTER_MS: i64 = 3_600_000;

/// What a `SelectMergeStacktraces` request selects.
#[derive(Debug, Clone, Copy, Default)]
pub struct TreeRequest<'a> {
    pub profile_type: &'a str,
    pub label_selector: &'a str,
    pub start_ms: i64,
    pub end_ms: i64,
    /// The request's `max_nodes`, `None` when absent.
    pub max_nodes: Option<i64>,
    /// `stack_trace_selector.call_site[].name`, root first.
    pub call_site: &'a [String],
    pub span_selector: &'a [String],
    pub trace_id_selector: &'a [String],
    pub profile_id_selector: &'a [String],
}

/// A planned request: S5's text and the two `max_nodes` the trim uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Planned {
    pub sql: String,
    /// The resolved value: steps 1 and 2 of the trim.
    pub max_nodes: i64,
    /// The request's value, 0 when absent: step 3 of the trim.
    pub raw_max_nodes: i64,
}

/// One node of the merged tree, as S5 returns it.
#[derive(Debug, Clone, PartialEq, Eq, clickhouse::Row, Serialize, Deserialize)]
pub struct TreeRow {
    pub node: u128,
    /// 0 for a root.
    pub parent: u128,
    pub name: String,
    pub total: i64,
    pub self_value: i64,
}

/// One selection's checked parts (issue #827): the series subquery, the
/// window, the matchers, and the span, trace and profile-id lines of the
/// sample read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Side {
    pub series: String,
    pub window: DataWindow,
    pub matchers: Vec<LabelMatcher>,
    pub filters: String,
}

impl Side {
    /// The type id the matchers select.
    pub(super) fn type_id(&self) -> &str {
        self.matchers
            .iter()
            .rev()
            .find(|m| m.key == "__profile_type__")
            .map(|m| m.value.as_str())
            .unwrap_or_default()
    }

    /// The stack rows' receive days the window's samples can have.
    pub(super) fn days(&self) -> (String, String) {
        (
            utc_date(self.window.start_ms.saturating_sub(STACK_DAY_BEFORE_MS)),
            utc_date(self.window.end_ms.saturating_add(STACK_DAY_AFTER_MS)),
        )
    }
}

/// Checks one selection in the reference's order — a span selector with a
/// trace selector, then the window, the type and the selector, then the id
/// lists — and renders its parts: `Ok(None)` when its window ends before
/// the lookback floor.
pub(super) fn side(
    tenant: &Tenant,
    config: &ProfilesReadConfig,
    req: &TreeRequest<'_>,
    now_ms: i64,
) -> Result<Option<Side>, ProfilesReadError> {
    refuse_span_with_trace(req.span_selector, req.trace_id_selector)?;
    let selectors = [req.label_selector];
    let compiled = compile(
        tenant,
        &config.series_table,
        &SelectorRequest {
            selectors: &selectors,
            profile_type: Some(req.profile_type),
            start_ms: req.start_ms,
            end_ms: req.end_ms,
        },
        now_ms,
        config.limits,
    )
    .map_err(|SelectorError::InvalidArgument(m)| ProfilesReadError::InvalidArgument(m))?;
    let (series, window, matchers) = match compiled {
        Compiled::Empty => return Ok(None),
        Compiled::Series {
            sql,
            window,
            matchers,
        } => (sql, window, matchers),
    };
    let invalid = ProfilesReadError::InvalidArgument;
    let spans = req
        .span_selector
        .iter()
        .map(|s| span_id(s))
        .collect::<Result<Vec<u64>, String>>()
        .map_err(invalid)?;
    let traces = req
        .trace_id_selector
        .iter()
        .map(|s| trace_id(s))
        .collect::<Result<Vec<String>, String>>()
        .map_err(invalid)?;
    let profiles = req
        .profile_id_selector
        .iter()
        .map(|s| uuid(s))
        .collect::<Result<Vec<String>, String>>()
        .map_err(invalid)?;
    let mut filters = String::new();
    if !spans.is_empty() {
        let list: Vec<String> = spans.iter().map(u64::to_string).collect();
        filters.push_str(&format!("\n      AND span_id IN ({})", list.join(", ")));
    }
    if !traces.is_empty() {
        let list: Vec<String> = traces.iter().map(|t| format!("unhex('{t}')")).collect();
        filters.push_str(&format!("\n      AND trace_id IN ({})", list.join(", ")));
    }
    if !profiles.is_empty() {
        let list: Vec<String> = profiles.iter().map(|p| format!("'{p}'")).collect();
        filters.push_str(&format!("\n      AND profile_id IN ({})", list.join(", ")));
    }
    Ok(Some(Side {
        series,
        window,
        matchers,
        filters,
    }))
}

/// Plans `req` for `tenant`: `Ok(None)` when its window ends before the
/// lookback floor, so nothing is read. The selection is checked by
/// [`side`], then `max_nodes`.
pub fn plan(
    tenant: &Tenant,
    config: &ProfilesReadConfig,
    req: &TreeRequest<'_>,
    now_ms: i64,
) -> Result<Option<Planned>, ProfilesReadError> {
    let Some(side) = side(tenant, config, req, now_ms)? else {
        return Ok(None);
    };
    let raw_max_nodes = req.max_nodes.unwrap_or(0);
    let max_nodes = resolve_max_nodes(raw_max_nodes, config)?;

    let tenant_term = TenantTerm::of(tenant);
    let call_site = if req.call_site.is_empty() {
        String::new()
    } else {
        format!(
            "\n      AND {}",
            call_site_predicate("names", req.call_site)
        )
    };
    let (d0, d1) = side.days();
    let sql = format!(
        "SELECT node_ids[i] AS node,
       any(if(i = 1, toUInt128(0), node_ids[i - 1])) AS parent,
       any(names[i]) AS name,
       sum(v) AS total,
       sumIf(v, i = length(node_ids)) AS self_value
FROM (
  SELECT st.stack_id AS stack_id, any(st.node_ids) AS node_ids, any(st.names) AS names, any(agg.v) AS v
  FROM (
    SELECT stack_id, node_ids, names
    FROM profile_stacks
    WHERE {tenant_term} AND day BETWEEN '{d0}' AND '{d1}'{call_site}
  ) AS st
  INNER JOIN (
    SELECT stack_id, sum(value) AS v
    FROM {samples}
    WHERE {tenant_term} AND type_id = {type_id}{service_terms}
      AND fingerprint IN ({series})
      AND timestamp_ns >= {start_ns} AND timestamp_ns <= {end_ns}{filters}
    GROUP BY stack_id
  ) AS agg ON st.stack_id = agg.stack_id
  GROUP BY st.stack_id
)
ARRAY JOIN arrayEnumerate(node_ids) AS i
GROUP BY node",
        samples = config.samples_table,
        type_id = ch_string(side.type_id()),
        service_terms = crate::metrics::profile_service_terms(&side.matchers),
        series = side.series,
        start_ns = side.window.start_ms.saturating_mul(1_000_000),
        end_ns = side.window.end_ms.saturating_mul(1_000_000),
        filters = side.filters,
    );
    Ok(Some(Planned {
        sql,
        max_nodes,
        raw_max_nodes,
    }))
}

/// The reference refuses a span selector with a trace selector before
/// anything else it checks.
pub fn refuse_span_with_trace(
    spans: &[String],
    traces: &[String],
) -> Result<(), ProfilesReadError> {
    if !spans.is_empty() && !traces.is_empty() {
        return Err(ProfilesReadError::InvalidArgument(
            "span_selector and trace_id_selector cannot be combined".to_string(),
        ));
    }
    Ok(())
}

/// `max_nodes` as the reference resolves it (`ValidateMaxNodes`): 0 is the
/// default; above the largest allowed, or negative, is refused.
pub(super) fn resolve_max_nodes(
    raw: i64,
    config: &ProfilesReadConfig,
) -> Result<i64, ProfilesReadError> {
    if raw == 0 {
        return Ok(config.max_nodes_default);
    }
    let max = config.max_nodes_max;
    if raw > max {
        return Err(ProfilesReadError::InvalidArgument(format!(
            "max flamegraph nodes limit {raw} is greater than allowed {max}"
        )));
    }
    if raw < 0 {
        return Err(ProfilesReadError::InvalidArgument(format!(
            "max flamegraph nodes limit must be set (max allowed {max})"
        )));
    }
    Ok(raw)
}

/// Issue #824: the stack rows a call site keeps — those whose frames,
/// `column`, begin with `names`, root first. Issue #827 passes the frames
/// as an aggregate of the joined stack rows.
pub(super) fn call_site_predicate(column: &str, names: &[String]) -> String {
    let names: Vec<String> = names.iter().map(|n| ch_string(n)).collect();
    format!(
        "arraySlice({column}, 1, {}) = [{}]",
        names.len(),
        names.join(", ")
    )
}

/// `ms` as its UTC date, `YYYY-MM-DD`.
pub(super) fn utc_date(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|t| t.format("%Y-%m-%d").to_string())
        .unwrap_or_default()
}

/// A span id as ingest stores it: 16 hex digits, 8 bytes read
/// little-endian (`NewSpanSelector`).
fn span_id(s: &str) -> Result<u64, String> {
    if s.len() != 16 {
        return Err(format!("invalid span id length: {s:?}"));
    }
    let mut b = [0u8; 8];
    hex_decode(s.as_bytes(), &mut b)?;
    Ok(u64::from_le_bytes(b))
}

/// A trace id: 32 hex digits, rendered in lower case (`DecodeTraceID`).
fn trace_id(s: &str) -> Result<String, String> {
    if s.len() != 32 {
        return Err(format!("invalid trace id length: {s:?}"));
    }
    let mut b = [0u8; 16];
    hex_decode(s.as_bytes(), &mut b)?;
    Ok(s.to_ascii_lowercase())
}

/// A UUID in any form the reference's parser reads — 36 characters, 38 in
/// braces, 45 with `urn:uuid:`, or 32 hex digits — rendered canonical and
/// lower case.
fn uuid(s: &str) -> Result<String, String> {
    const FORMAT: &str = "invalid UUID format";
    let b = s.as_bytes();
    let mut out = [0u8; 16];
    let hyphenated = match b.len() {
        36 => b,
        45 => {
            if !b[..9].eq_ignore_ascii_case(b"urn:uuid:") {
                let prefix = String::from_utf8_lossy(&b[..9]);
                return Err(format!("invalid urn prefix: {prefix:?}"));
            }
            &b[9..]
        }
        38 => &b[1..],
        32 => {
            for (i, o) in out.iter_mut().enumerate() {
                *o = hex_pair(b[2 * i], b[2 * i + 1]).ok_or(FORMAT)?;
            }
            return Ok(canonical(&out));
        }
        n => return Err(format!("invalid UUID length: {n}")),
    };
    if hyphenated[8] != b'-'
        || hyphenated[13] != b'-'
        || hyphenated[18] != b'-'
        || hyphenated[23] != b'-'
    {
        return Err(FORMAT.to_string());
    }
    const AT: [usize; 16] = [0, 2, 4, 6, 9, 11, 14, 16, 19, 21, 24, 26, 28, 30, 32, 34];
    for (o, x) in out.iter_mut().zip(AT) {
        *o = hex_pair(hyphenated[x], hyphenated[x + 1]).ok_or(FORMAT)?;
    }
    Ok(canonical(&out))
}

fn canonical(b: &[u8; 16]) -> String {
    let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
}

fn hex_digit(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

fn hex_pair(hi: u8, lo: u8) -> Option<u8> {
    Some(hex_digit(hi)? << 4 | hex_digit(lo)?)
}

/// Hex digits into `out`, refused at the first byte that is not one with
/// the reference's text (`encoding/hex`'s `InvalidByteError`).
fn hex_decode(src: &[u8], out: &mut [u8]) -> Result<(), String> {
    let digit = |c: u8| hex_digit(c).ok_or_else(|| invalid_byte(c));
    for (i, o) in out.iter_mut().enumerate() {
        *o = digit(src[2 * i])? << 4 | digit(src[2 * i + 1])?;
    }
    Ok(())
}

/// `encoding/hex: invalid byte: %#U`: the code point, and the character
/// quoted when it prints.
fn invalid_byte(c: u8) -> String {
    let ch = char::from(c);
    if ch == ' ' || (!ch.is_control() && !ch.is_whitespace() && ch != '\u{ad}') {
        format!("encoding/hex: invalid byte: U+{c:04X} '{ch}'")
    } else {
        format!("encoding/hex: invalid byte: U+{c:04X}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profiles::selector::WindowLimits;
    use crate::profiles::tenant_scope;
    use crate::traces::tenant_scope::TenantTerm;

    const CPU: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
    /// 2026-11-16T09:00Z and 10:00Z.
    const START: i64 = 1_794_819_600_000;
    const END: i64 = 1_794_823_200_000;
    const DAY_MS: i64 = 86_400_000;
    const HOUR_MS: i64 = 3_600_000;

    /// Plan §0's statement.
    const EXAMPLE: &str = "SELECT node_ids[i] AS node,
       any(if(i = 1, toUInt128(0), node_ids[i - 1])) AS parent,
       any(names[i]) AS name,
       sum(v) AS total,
       sumIf(v, i = length(node_ids)) AS self_value
FROM (
  SELECT st.stack_id AS stack_id, any(st.node_ids) AS node_ids, any(st.names) AS names, any(agg.v) AS v
  FROM (
    SELECT stack_id, node_ids, names
    FROM profile_stacks
    WHERE org_id = 't1' AND day BETWEEN '2026-11-16' AND '2026-11-16'
  ) AS st
  INNER JOIN (
    SELECT stack_id, sum(value) AS v
    FROM profile_samples
    WHERE org_id = 't1' AND type_id = 'process_cpu:cpu:nanoseconds:cpu:nanoseconds'
      AND service = 'checkout'
      AND fingerprint IN (SELECT fingerprint
FROM profile_series
WHERE org_id = 't1' AND day BETWEEN '2026-11-16' AND '2026-11-16'
  AND bitAnd(hours, multiIf(day = '2026-11-16' AND day = '2026-11-16', 1536, day = '2026-11-16', 16776704, day = '2026-11-16', 2047, 16777215)) != 0
  AND service = 'checkout'
  AND type_id = 'process_cpu:cpu:nanoseconds:cpu:nanoseconds')
      AND timestamp_ns >= 1794819600000000000 AND timestamp_ns <= 1794823200000000000
    GROUP BY stack_id
  ) AS agg ON st.stack_id = agg.stack_id
  GROUP BY st.stack_id
)
ARRAY JOIN arrayEnumerate(node_ids) AS i
GROUP BY node";

    fn tenant(name: &str) -> Tenant {
        let value = http::HeaderValue::from_str(name).expect("a header value");
        Tenant::from_header(Some(&value), false).expect("a valid tenant")
    }

    fn single() -> ProfilesReadConfig {
        ProfilesReadConfig {
            series_table: "profile_series".to_string(),
            samples_table: "profile_samples".to_string(),
            entries_table: "profile_entries".to_string(),
            dist_suffix: String::new(),
            limits: WindowLimits::default(),
            scan_budget_rows: 50_000_000,
            max_tree_nodes: 4_194_304,
            max_result_bytes: 268_435_456,
            query_memory_bytes: 4_294_967_296,
            distributed: false,
            skip_unavailable_shards: false,
            max_nodes_default: 8_192,
            max_nodes_max: 1_048_576,
        }
    }

    fn clustered() -> ProfilesReadConfig {
        ProfilesReadConfig {
            series_table: "profile_series_dist".to_string(),
            samples_table: "profile_samples_dist".to_string(),
            entries_table: "profile_entries_dist".to_string(),
            dist_suffix: "_dist".to_string(),
            distributed: true,
            ..single()
        }
    }

    fn example() -> TreeRequest<'static> {
        TreeRequest {
            profile_type: CPU,
            label_selector: r#"{service_name="checkout"}"#,
            start_ms: START,
            end_ms: END,
            ..TreeRequest::default()
        }
    }

    fn planned(config: &ProfilesReadConfig, req: &TreeRequest<'_>, now: i64) -> Planned {
        match plan(&tenant("t1"), config, req, now) {
            Ok(Some(p)) => p,
            other => panic!("expected a plan, got {other:?}"),
        }
    }

    fn refused(req: &TreeRequest<'_>) -> String {
        match plan(&tenant("t1"), &single(), req, END) {
            Err(ProfilesReadError::InvalidArgument(m)) => m,
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    fn strings(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    /// T1: plan §0's request renders plan §0's statement, byte for byte.
    #[test]
    fn t1_the_example_renders_the_example_statement() {
        let p = planned(&single(), &example(), END);
        assert_eq!(p.sql, EXAMPLE);
    }

    /// T2: clustered, the samples and the series are read through their
    /// wrappers; the stacks, copied to every node, are read locally.
    #[test]
    fn t2_clustered_reads_the_samples_wrapper() {
        let sql = planned(&clustered(), &example(), END).sql;
        for expected in [
            "\n    FROM profile_samples_dist\n",
            "\nFROM profile_series_dist\n",
            "\n    FROM profile_stacks\n",
        ] {
            assert!(sql.contains(expected), "{expected:?} missing from\n{sql}");
        }
        assert!(!sql.contains("profile_stacks_dist"), "{sql}");
    }

    /// T3: the tenant check accepts both renderings.
    #[test]
    fn t3_the_tenant_check_accepts_both() {
        let term = TenantTerm::of(&tenant("t1"));
        let one = planned(&single(), &example(), END).sql;
        assert_eq!(
            tenant_scope::check(&one, &term, &tenant_scope::tables("")),
            Ok(())
        );
        let two = planned(&clustered(), &example(), END).sql;
        assert_eq!(
            tenant_scope::check(&two, &term, &tenant_scope::tables("_dist")),
            Ok(())
        );
    }

    /// T4: each filter renders its line.
    #[test]
    fn t4_each_filter_renders_its_line() {
        let call_site = strings(&["main", "a"]);
        let sql = planned(
            &single(),
            &TreeRequest {
                call_site: &call_site,
                ..example()
            },
            END,
        )
        .sql;
        assert!(
            sql.contains(
                "WHERE org_id = 't1' AND day BETWEEN '2026-11-16' AND '2026-11-16'\n      \
                 AND arraySlice(names, 1, 2) = ['main', 'a']\n  ) AS st"
            ),
            "{sql}"
        );
        let spans = strings(&["00f0000000000001"]);
        let sql = planned(
            &single(),
            &TreeRequest {
                span_selector: &spans,
                ..example()
            },
            END,
        )
        .sql;
        assert!(
            sql.contains(
                "timestamp_ns <= 1794823200000000000\n      AND span_id IN (72057594037989376)\n"
            ),
            "{sql}"
        );
        let traces = strings(&["0102030405060708090A0b0c0d0e0f10"]);
        let sql = planned(
            &single(),
            &TreeRequest {
                trace_id_selector: &traces,
                ..example()
            },
            END,
        )
        .sql;
        assert!(
            sql.contains(
                "timestamp_ns <= 1794823200000000000\n      \
                 AND trace_id IN (unhex('0102030405060708090a0b0c0d0e0f10'))\n"
            ),
            "{sql}"
        );
        let ids = strings(&[
            "7C9E6679-7425-40DE-944B-E07FC1F90AE7",
            "0190f5a41b7e7cc08000000000000001",
        ]);
        let sql = planned(
            &single(),
            &TreeRequest {
                profile_id_selector: &ids,
                ..example()
            },
            END,
        )
        .sql;
        assert!(
            sql.contains(
                "timestamp_ns <= 1794823200000000000\n      AND profile_id IN \
                 ('7c9e6679-7425-40de-944b-e07fc1f90ae7', \
                 '0190f5a4-1b7e-7cc0-8000-000000000001')\n"
            ),
            "{sql}"
        );
    }

    /// T5: each refusal carries the reference's text.
    #[test]
    fn t5_refusals_carry_the_reference_text() {
        let span = strings(&["0000000000000001"]);
        let trace = strings(&["0102030405060708090a0b0c0d0e0f10"]);
        assert_eq!(
            refused(&TreeRequest {
                span_selector: &span,
                trace_id_selector: &trace,
                ..example()
            }),
            "span_selector and trace_id_selector cannot be combined"
        );
        let short = strings(&["000000000000001"]);
        assert_eq!(
            refused(&TreeRequest {
                span_selector: &short,
                ..example()
            }),
            r#"invalid span id length: "000000000000001""#
        );
        let bad = strings(&["000000000000000g"]);
        assert_eq!(
            refused(&TreeRequest {
                span_selector: &bad,
                ..example()
            }),
            "encoding/hex: invalid byte: U+0067 'g'"
        );
        let short = strings(&["0102030405060708090a0b0c0d0e0f1"]);
        assert_eq!(
            refused(&TreeRequest {
                trace_id_selector: &short,
                ..example()
            }),
            r#"invalid trace id length: "0102030405060708090a0b0c0d0e0f1""#
        );
        let uuid = strings(&["abc"]);
        assert_eq!(
            refused(&TreeRequest {
                profile_id_selector: &uuid,
                ..example()
            }),
            "invalid UUID length: 3"
        );
        let uuid = strings(&["7c9e6679x7425-40de-944b-e07fc1f90ae7"]);
        assert_eq!(
            refused(&TreeRequest {
                profile_id_selector: &uuid,
                ..example()
            }),
            "invalid UUID format"
        );
        assert_eq!(
            refused(&TreeRequest {
                max_nodes: Some(1_048_577),
                ..example()
            }),
            "max flamegraph nodes limit 1048577 is greater than allowed 1048576"
        );
        assert_eq!(
            refused(&TreeRequest {
                max_nodes: Some(-1),
                ..example()
            }),
            "max flamegraph nodes limit must be set (max allowed 1048576)"
        );
    }

    /// T6: an absent or zero `max_nodes` resolves to the default and leaves
    /// step 3's value 0; another value is both.
    #[test]
    fn t6_max_nodes_resolves() {
        for max_nodes in [None, Some(0)] {
            let p = planned(
                &single(),
                &TreeRequest {
                    max_nodes,
                    ..example()
                },
                END,
            );
            assert_eq!((p.max_nodes, p.raw_max_nodes), (8_192, 0), "{max_nodes:?}");
        }
        let p = planned(
            &single(),
            &TreeRequest {
                max_nodes: Some(4),
                ..example()
            },
            END,
        );
        assert_eq!((p.max_nodes, p.raw_max_nodes), (4, 4));
    }

    /// T7: the stack read covers the receive days the window's samples can
    /// have: 10 minutes before its start to 1 hour after its end.
    #[test]
    fn t7_the_stack_days_bound_the_receive_days() {
        // Day d is 2026-11-16, d0 its midnight.
        let d0 = START - 9 * HOUR_MS;
        for (start, end, days) in [
            (
                d0 + 23 * HOUR_MS,
                d0 + 23 * HOUR_MS + 59 * 60_000,
                "'2026-11-16' AND '2026-11-17'",
            ),
            (
                d0 + 5 * 60_000,
                d0 + 20 * 60_000,
                "'2026-11-15' AND '2026-11-16'",
            ),
            (START, END, "'2026-11-16' AND '2026-11-16'"),
        ] {
            let sql = planned(
                &single(),
                &TreeRequest {
                    start_ms: start,
                    end_ms: end,
                    ..example()
                },
                end,
            )
            .sql;
            let expected =
                format!("FROM profile_stacks\n    WHERE org_id = 't1' AND day BETWEEN {days}\n");
            assert!(sql.contains(&expected), "{expected:?} missing from\n{sql}");
        }
    }

    /// T8: a regex `service_name` matcher is repeated on the sample read.
    #[test]
    fn t8_a_regex_service_matcher_is_repeated() {
        let sql = planned(
            &single(),
            &TreeRequest {
                label_selector: r#"{service_name=~"check.*"}"#,
                ..example()
            },
            END,
        )
        .sql;
        assert!(
            sql.contains(
                "WHERE org_id = 't1' AND type_id = 'process_cpu:cpu:nanoseconds:cpu:nanoseconds'\n      \
                 AND match(service, '(?-s)^(?:check.*)$')\n      AND fingerprint IN ("
            ),
            "{sql}"
        );
    }

    /// T9: a window ending before the lookback floor plans nothing.
    #[test]
    fn t9_a_window_before_the_floor_plans_nothing() {
        let now = END + 8 * DAY_MS;
        assert_eq!(plan(&tenant("t1"), &single(), &example(), now), Ok(None));
        // The same request inside the lookback plans a statement.
        assert!(matches!(
            plan(&tenant("t1"), &single(), &example(), END),
            Ok(Some(_))
        ));
    }
}
