//! Issue #820: a profiles request's label selector, profile type and window
//! compiled to SERIES — one subquery over the series table, scoped to the
//! tenant, that a route places inside its one statement as
//! `fingerprint IN (…)`.

use pulsus_clickhouse::ChError;
use pulsus_model::{LabelMatcher, MatchOp, Tenant};

use crate::metrics::DataWindow;

/// The reference's default longest window, 24 hours.
pub const MAX_QUERY_LENGTH_MS: i64 = 86_400_000;
/// The reference's default lookback, 7 days.
pub const MAX_QUERY_LOOKBACK_MS: i64 = 604_800_000;

/// The two time limits a window is checked against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowLimits {
    pub max_length_ms: i64,
    pub max_lookback_ms: i64,
}

impl Default for WindowLimits {
    fn default() -> Self {
        WindowLimits {
            max_length_ms: MAX_QUERY_LENGTH_MS,
            max_lookback_ms: MAX_QUERY_LOOKBACK_MS,
        }
    }
}

/// What a request selects.
#[derive(Debug, Clone, Copy)]
pub struct SelectorRequest<'a> {
    /// The typed routes' one `label_selector`, or the label routes'
    /// `matchers` (none = `{}`); several are AND-ed.
    pub selectors: &'a [&'a str],
    /// The typed routes' `profile_type_id`; `None` on the label routes.
    pub profile_type: Option<&'a str>,
    pub start_ms: i64,
    pub end_ms: i64,
}

/// A compiled request.
#[derive(Debug, PartialEq, Eq)]
pub enum Compiled {
    /// The window ends before the lookback floor: answer empty, send nothing.
    Empty,
    /// SERIES, the window the outer read bounds its own time column by,
    /// and the matchers SERIES renders (issue #821: the catalogue reads
    /// render them under their own `SELECT`), the type's matcher last.
    Series {
        sql: String,
        window: DataWindow,
        matchers: Vec<LabelMatcher>,
    },
}

/// A request the reference refuses as an invalid argument.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectorError {
    InvalidArgument(String),
}

/// Compiles `req` for `tenant` over `series_table` (`profile_series`, or
/// its wrapper on a cluster), in the reference's order: the window, then
/// the profile type, then the selectors. Each refusal carries the
/// reference's text.
///
/// Every selector regex is left to the database: rendered anchored, with
/// its compile probe, and never compiled here.
pub fn compile(
    tenant: &Tenant,
    series_table: &str,
    req: &SelectorRequest<'_>,
    now_ms: i64,
    limits: WindowLimits,
) -> Result<Compiled, SelectorError> {
    let invalid = |m: String| Err(SelectorError::InvalidArgument(m));
    let end_ms = req.end_ms;
    if req.start_ms == 0 || end_ms == 0 {
        return invalid("missing time range in the query".to_string());
    }
    if req.start_ms > end_ms {
        return invalid("query start time is after end time".to_string());
    }
    let floor = now_ms.saturating_sub(limits.max_lookback_ms);
    if end_ms < floor {
        return Ok(Compiled::Empty);
    }
    let start_ms = req.start_ms.max(floor);
    let length = end_ms - start_ms;
    if length > limits.max_length_ms {
        return invalid(format!(
            "the query time range exceeds the limit (max_query_length, actual: {}, limit: {})",
            crate::traces::go_duration_string(length.saturating_mul(1_000_000)),
            model_duration(limits.max_length_ms)
        ));
    }
    let type_matcher = match req.profile_type {
        Some(id) => Some(profile_type_matcher(id).map_err(SelectorError::InvalidArgument)?),
        None => None,
    };
    let mut matchers = Vec::new();
    for s in req.selectors {
        let parsed = pulsus_promql::metric_selector(s).map_err(|e| {
            SelectorError::InvalidArgument(format!("parsing label selector {s:?}: {e}"))
        })?;
        matchers.extend(parsed);
    }
    matchers.extend(type_matcher);
    let window = DataWindow { start_ms, end_ms };
    Ok(Compiled::Series {
        sql: crate::metrics::profile_series(tenant, window, series_table, &matchers),
        window,
        matchers,
    })
}

/// `Some(InvalidArgument(detail))` for a statement refused with code 427 —
/// a selector regex RE2 cannot compile; `None` for every other error.
pub fn read_error(e: &ChError) -> Option<SelectorError> {
    crate::metrics::regex_rejection_detail(e).map(SelectorError::InvalidArgument)
}

/// A profile type id as the reference reads it: five parts, or six with a
/// trailing `delta`, selected by its first five.
fn profile_type_matcher(id: &str) -> Result<LabelMatcher, String> {
    let parts: Vec<&str> = id.split(':').collect();
    if parts.len() != 5 && parts.len() != 6 {
        return Err(format!(
            "profile-type selection must be of the form \
             <name>:<sample-type>:<sample-unit>:<period-type>:<period-unit>(:delta), \
             got({}): {id:?}",
            parts.len()
        ));
    }
    Ok(LabelMatcher {
        key: "__profile_type__".to_string(),
        op: MatchOp::Eq,
        value: parts[..5].join(":"),
    })
}

/// A millisecond count as the reference's duration text: years and weeks
/// only when they divide it exactly, then days, hours, minutes, seconds and
/// milliseconds (24 h is `1d`).
fn model_duration(ms: i64) -> String {
    const UNITS: [(&str, i64, bool); 7] = [
        ("y", 365 * 86_400_000, true),
        ("w", 7 * 86_400_000, true),
        ("d", 86_400_000, false),
        ("h", 3_600_000, false),
        ("m", 60_000, false),
        ("s", 1_000, false),
        ("ms", 1, false),
    ];
    if ms == 0 {
        return "0s".to_string();
    }
    let mut rest = ms;
    let mut out = String::new();
    for (unit, mult, exact) in UNITS {
        if exact && rest % mult != 0 {
            continue;
        }
        let v = rest / mult;
        if v > 0 {
            out.push_str(&format!("{v}{unit}"));
            rest -= v * mult;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profiles::tenant_scope;
    use crate::traces::tenant_scope::TenantTerm;

    const CPU: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
    /// `W`: 2026-11-15T11:00Z to 12:00Z; `now_ms` is its end.
    const W_START: i64 = 1_794_740_400_000;
    const W_END: i64 = 1_794_744_000_000;
    const HOUR_MS: i64 = 3_600_000;
    const DAY_MS: i64 = 86_400_000;

    const S1_TEXT: &str = "SELECT fingerprint
FROM profile_series
WHERE org_id = 't1' AND day BETWEEN '2026-11-15' AND '2026-11-15'
  AND bitAnd(hours, multiIf(day = '2026-11-15' AND day = '2026-11-15', 6144, day = '2026-11-15', 16775168, day = '2026-11-15', 8191, 16777215)) != 0
  AND service = 'a'
  AND JSONExtractString(labels, 'env') != 'p'
  AND match(JSONExtractString(labels, 'x'), '(?-s)^(?:y.*)$')
  AND type_id = 'process_cpu:cpu:nanoseconds:cpu:nanoseconds'
  AND 0 * match('', '(?-s)^(?:y.*)$') = 0";

    fn t1() -> Tenant {
        let value = http::HeaderValue::from_static("t1");
        Tenant::from_header(Some(&value), false).expect("a valid tenant")
    }

    fn run(
        selectors: &[&str],
        profile_type: Option<&str>,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Compiled, SelectorError> {
        compile(
            &t1(),
            "profile_series",
            &SelectorRequest {
                selectors,
                profile_type,
                start_ms,
                end_ms,
            },
            W_END,
            WindowLimits::default(),
        )
    }

    fn series(r: Result<Compiled, SelectorError>) -> (String, DataWindow) {
        match r {
            Ok(Compiled::Series { sql, window, .. }) => (sql, window),
            other => panic!("expected Series, got {other:?}"),
        }
    }

    fn refused(r: Result<Compiled, SelectorError>) -> String {
        match r {
            Err(SelectorError::InvalidArgument(m)) => m,
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    /// S1: the design's selector, type and window, byte for byte.
    #[test]
    fn s1_the_selector_compiles_to_the_series_subquery() {
        let (sql, window) = series(run(
            &[r#"{service_name="a",env!="p",x=~"y.*"}"#],
            Some(CPU),
            W_START,
            W_END,
        ));
        assert_eq!(sql, S1_TEXT);
        assert_eq!(
            window,
            DataWindow {
                start_ms: W_START,
                end_ms: W_END
            }
        );
    }

    /// S2: the tenant check accepts what the compiler renders.
    #[test]
    fn s2_the_tenant_check_accepts_the_compiled_text() {
        let (sql, _) = series(run(
            &[r#"{service_name="a",env!="p",x=~"y.*"}"#],
            Some(CPU),
            W_START,
            W_END,
        ));
        assert_eq!(
            tenant_scope::check(&sql, &TenantTerm::of(&t1()), &tenant_scope::tables("")),
            Ok(())
        );
    }

    /// S3: no selector and no type is `{}`: the window alone.
    #[test]
    fn s3_no_selector_reads_the_window_alone() {
        let (sql, _) = series(run(&[], None, W_START, W_END));
        let first_four: Vec<&str> = S1_TEXT.lines().take(4).collect();
        assert_eq!(sql, first_four.join("\n"));
    }

    /// S4: a type of five or six parts selects its first five; any other
    /// count is refused with the reference's text.
    #[test]
    fn s4_the_profile_type_has_five_or_six_parts() {
        let delta = format!("{CPU}:delta");
        let (sql, _) = series(run(&[], Some(&delta), W_START, W_END));
        assert_eq!(
            sql.lines().last(),
            Some(&*format!("  AND type_id = '{CPU}'"))
        );
        assert_eq!(
            refused(run(&[], Some("a:b:c:d"), W_START, W_END)),
            "profile-type selection must be of the form \
             <name>:<sample-type>:<sample-unit>:<period-type>:<period-unit>(:delta), \
             got(4): \"a:b:c:d\""
        );
    }

    /// S5: the length limit is checked after the lookback clamp, a window
    /// wholly before the floor is `Empty`, and the two range errors.
    #[test]
    fn s5_the_window_is_clamped_then_limited() {
        let n = W_END;
        series(run(&[], None, n - DAY_MS, n));
        assert_eq!(
            refused(run(&[], None, n - DAY_MS - 1, n)),
            "the query time range exceeds the limit (max_query_length, \
             actual: 24h0m0.001s, limit: 1d)"
        );
        let (_, window) = series(run(&[], None, n - 8 * DAY_MS, n - 7 * DAY_MS + 2 * HOUR_MS));
        assert_eq!(window.start_ms, n - 7 * DAY_MS);
        assert_eq!(
            run(&[], None, n - 9 * DAY_MS, n - 8 * DAY_MS),
            Ok(Compiled::Empty)
        );
        assert_eq!(
            refused(run(&[], None, 0, n)),
            "missing time range in the query"
        );
        assert_eq!(
            refused(run(&[], None, n, n - 1)),
            "query start time is after end time"
        );
    }

    /// S6: several selectors are AND-ed in order; a malformed one is refused
    /// naming it.
    #[test]
    fn s6_several_selectors_are_and_ed_in_order() {
        let (sql, _) = series(run(&[r#"{a="1"}"#, r#"{b="2"}"#], None, W_START, W_END));
        let lines: Vec<&str> = sql.lines().collect();
        assert_eq!(
            lines[4..],
            [
                "  AND JSONExtractString(labels, 'a') = '1'",
                "  AND JSONExtractString(labels, 'b') = '2'",
            ]
        );
        let err = refused(run(&["{a="], None, W_START, W_END));
        assert!(err.starts_with(r#"parsing label selector "{a=""#), "{err}");
    }

    /// S8: `!~` renders `NOT match` and its probe.
    #[test]
    fn s8_a_negative_regex_renders_not_match_and_its_probe() {
        let (sql, _) = series(run(&[r#"{env!~"prod"}"#], None, W_START, W_END));
        let lines: Vec<&str> = sql.lines().collect();
        assert_eq!(
            lines.get(4).copied(),
            Some("  AND NOT match(JSONExtractString(labels, 'env'), '(?-s)^(?:prod)$')")
        );
        assert_eq!(
            lines.last().copied(),
            Some("  AND 0 * match('', '(?-s)^(?:prod)$') = 0")
        );
    }

    /// S7: a 427 becomes an invalid argument holding RE2's reason; another
    /// code is not this mapping's.
    #[test]
    fn s7_a_rejected_regex_is_an_invalid_argument() {
        let e = ChError::Server {
            code: 427,
            message: "Code: 427. DB::Exception: OptimizedRegularExpression: cannot compile re2: \
                      (?-s)^(?:()$, error: missing ): (?-s)^(?:()$. Look at the syntax page for \
                      reference. (CANNOT_COMPILE_REGEXP)"
                .to_string(),
        };
        match read_error(&e) {
            Some(SelectorError::InvalidArgument(d)) => {
                assert!(d.starts_with("^(?:()$, error: missing )"), "{d}")
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
        let memory = ChError::Server {
            code: 241,
            message: "Code: 241. DB::Exception: Memory limit exceeded".to_string(),
        };
        assert_eq!(read_error(&memory), None);
    }
}
