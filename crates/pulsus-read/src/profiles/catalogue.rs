//! Issue #821: the profiles catalogue reads — profile types, label names,
//! label values and series — as one statement each over the series table.
//!
//! Every statement is SERIES's `FROM` and `WHERE` lines (#820's leaf,
//! [`crate::metrics::profile_series_where`]) under the method's own
//! `SELECT`, so a series is selected by its tenant, its day partitions, its
//! hour mask (owner, 2026-11-15, "By the hour") and the request's matchers,
//! and no other table is read.
//!
//! | Id | `SELECT` | order |
//! |---|---|---|
//! | S1 | `DISTINCT type_id AS s`, no matcher | `s` |
//! | S2 | `DISTINCT arrayJoin(<keys>) AS s` | `s` |
//! | S3 | `DISTINCT JSONExtractString(labels, <name>) AS s`, `s != ''` | `s` |
//! | S4 | `DISTINCT <pairs> AS ls` | `ls` |
//!
//! Without the client's `allow-utf8-labelnames` capability, S2 and S4 keep
//! only the names the reference's legacy rule accepts
//! (`model/metric.go:184-196` of the Go module the reference pins), in the
//! statement, so S4 deduplicates after the filter.

use pulsus_model::{LabelMatcher, Tenant};

use crate::logql::escape::ch_string;

use super::engine::ProfilesReadError;
use super::selector::{Compiled, SelectorError, SelectorRequest, WindowLimits, compile};

/// The reference's legacy label-name rule, as a statement literal.
const LEGACY_NAME: &str = "'^[a-zA-Z_][a-zA-Z0-9_]*$'";

/// The reference's five-name `Series` request (`query_series_labels_compat.go:31-37`),
/// sorted.
const FIVE_NAMES: [&str; 5] = [
    "__name__",
    "__profile_type__",
    "__type__",
    "pyroscope_app",
    "service_name",
];

/// The pairs the five-name request reads.
const TYPE_AND_SERVICE: &str = "has(['__profile_type__', 'service_name'], kv.1)";

/// The reference's default `ProfileTypes` window, the last hour
/// (`query_profile_types.go:31-35`).
const DEFAULT_TYPES_WINDOW_MS: i64 = 3_600_000;

/// The method a catalogue request is.
#[derive(Debug, Clone, Copy)]
pub enum Method<'a> {
    ProfileTypes,
    LabelNames {
        utf8: bool,
    },
    LabelValues {
        name: &'a str,
    },
    Series {
        label_names: &'a [String],
        utf8: bool,
    },
}

/// A catalogue request: its method, its matchers (ignored by
/// `ProfileTypes`) and its window in milliseconds.
#[derive(Debug, Clone, Copy)]
pub struct CatalogueRequest<'a> {
    pub method: Method<'a>,
    pub matchers: &'a [&'a str],
    pub start_ms: i64,
    pub end_ms: i64,
}

/// The one statement a request sends: a list of strings (S1–S3), or label
/// sets (S4). `compat` marks the five-name `Series` request, whose rows the
/// engine rebuilds as the reference does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Read {
    Strings(String),
    Series { sql: String, compat: bool },
}

impl Read {
    /// The statement's text.
    pub fn sql(&self) -> &str {
        match self {
            Read::Strings(sql) | Read::Series { sql, .. } => sql,
        }
    }
}

/// Plans `req` for `tenant` over `series_table`: `None` when the window ends
/// before the lookback floor (the answer is empty and nothing is sent), else
/// the one statement. Every refusal carries the reference's text.
pub fn plan(
    tenant: &Tenant,
    series_table: &str,
    limits: WindowLimits,
    req: &CatalogueRequest<'_>,
    now_ms: i64,
) -> Result<Option<Read>, ProfilesReadError> {
    let (mut start_ms, mut end_ms) = (req.start_ms, req.end_ms);
    let selectors: &[&str] = match req.method {
        Method::ProfileTypes => {
            if start_ms == 0 && end_ms == 0 {
                start_ms = now_ms - DEFAULT_TYPES_WINDOW_MS;
                end_ms = now_ms;
            }
            &[]
        }
        _ => req.matchers,
    };
    let compiled = compile(
        tenant,
        series_table,
        &SelectorRequest {
            selectors,
            profile_type: None,
            start_ms,
            end_ms,
        },
        now_ms,
        limits,
    )
    .map_err(|SelectorError::InvalidArgument(m)| ProfilesReadError::InvalidArgument(m))?;
    let (window, matchers) = match compiled {
        Compiled::Empty => return Ok(None),
        Compiled::Series {
            window, matchers, ..
        } => (window, matchers),
    };
    let from_where = |ms: &[LabelMatcher]| {
        crate::metrics::profile_series_where(tenant, window, series_table, ms)
    };
    Ok(Some(match req.method {
        Method::ProfileTypes => Read::Strings(format!(
            "SELECT DISTINCT type_id AS s\n{}\nORDER BY s",
            from_where(&[])
        )),
        Method::LabelNames { utf8 } => {
            let keys = if utf8 {
                "JSONExtractKeys(labels)".to_string()
            } else {
                format!("arrayFilter(n -> match(n, {LEGACY_NAME}), JSONExtractKeys(labels))")
            };
            Read::Strings(format!(
                "SELECT DISTINCT arrayJoin({keys}) AS s\n{}\nORDER BY s",
                from_where(&matchers)
            ))
        }
        Method::LabelValues { name } => {
            if name.is_empty() {
                return Err(ProfilesReadError::InvalidArgument(
                    "name is required".to_string(),
                ));
            }
            Read::Strings(format!(
                "SELECT DISTINCT JSONExtractString(labels, {}) AS s\n{}\n  AND s != ''\nORDER BY s",
                ch_string(name),
                from_where(&matchers)
            ))
        }
        Method::Series { label_names, utf8 } => {
            let compat = matchers.is_empty() && is_five_names(label_names);
            let names: Vec<&String> = label_names
                .iter()
                .filter(|n| utf8 || is_legacy_name(n))
                .collect();
            let keep = if compat {
                Some(TYPE_AND_SERVICE.to_string())
            } else if !names.is_empty() {
                let list: Vec<String> = names.iter().map(|n| ch_string(n)).collect();
                Some(format!("has([{}], kv.1)", list.join(", ")))
            } else if !utf8 {
                Some(format!("match(kv.1, {LEGACY_NAME})"))
            } else {
                None
            };
            let pairs = "JSONExtractKeysAndValues(labels, 'String')";
            let select = match keep {
                Some(keep) => format!("arrayFilter(kv -> {keep}, {pairs})"),
                None => pairs.to_string(),
            };
            Read::Series {
                sql: format!(
                    "SELECT DISTINCT {select} AS ls\n{}\nORDER BY ls",
                    from_where(&matchers)
                ),
                compat,
            }
        }
    }))
}

/// The reference's five-name list, in any order.
fn is_five_names(names: &[String]) -> bool {
    let mut sorted: Vec<&str> = names.iter().map(String::as_str).collect();
    sorted.sort_unstable();
    sorted == FIVE_NAMES
}

/// The reference's legacy label-name rule, `^[a-zA-Z_][a-zA-Z0-9_]*$`.
pub fn is_legacy_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    matches!(bytes.next(), Some(b) if b == b'_' || b.is_ascii_alphabetic())
        && bytes.all(|b| b == b'_' || b.is_ascii_alphanumeric())
}

/// The five-name `Series` answer built from S4's rows of `__profile_type__`
/// and `service_name` pairs, as the reference builds it
/// (`query_series_labels_compat.go:105-129`): four pairs per row — the type,
/// the service, the type's name and its sample type — sorted by name; a row
/// lacking either pair, or whose type has not five or six parts, is dropped;
/// the sets in `CompareLabelPairs` order.
pub fn compat_sets(rows: Vec<Vec<(String, String)>>) -> Vec<Vec<(String, String)>> {
    let mut out: Vec<Vec<(String, String)>> = rows
        .into_iter()
        .filter_map(|row| {
            let find = |key: &str| {
                row.iter()
                    .find(|(k, _)| k == key)
                    .map(|(_, v)| v.clone())
                    .filter(|v| !v.is_empty())
            };
            let service = find("service_name")?;
            let id = find("__profile_type__")?;
            let parts: Vec<&str> = id.split(':').collect();
            if parts.len() != 5 && parts.len() != 6 {
                return None;
            }
            let mut set = vec![
                ("__profile_type__".to_string(), id.clone()),
                ("service_name".to_string(), service),
                ("__name__".to_string(), parts[0].to_string()),
                ("__type__".to_string(), parts[1].to_string()),
            ];
            set.sort_by(|a, b| a.0.cmp(&b.0));
            Some(set)
        })
        .collect();
    out.sort();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profiles::tenant_scope;
    use crate::traces::tenant_scope::TenantTerm;

    /// §0's window, 2026-11-15T12:10Z to 12:20Z; `now` is its end.
    const START: i64 = 1_794_744_600_000;
    const END: i64 = 1_794_745_200_000;

    const WHERE: &str = "WHERE org_id = 't1' AND day BETWEEN '2026-11-15' AND '2026-11-15'
  AND bitAnd(hours, multiIf(day = '2026-11-15' AND day = '2026-11-15', 4096, day = '2026-11-15', 16773120, day = '2026-11-15', 8191, 16777215)) != 0";

    const LEGACY: &str = "'^[a-zA-Z_][a-zA-Z0-9_]*$'";

    fn t1() -> Tenant {
        let value = http::HeaderValue::from_static("t1");
        Tenant::from_header(Some(&value), false).expect("a valid tenant")
    }

    fn render(table: &str, method: Method<'_>, matchers: &[&str]) -> Read {
        plan(
            &t1(),
            table,
            WindowLimits::default(),
            &CatalogueRequest {
                method,
                matchers,
                start_ms: START,
                end_ms: END,
            },
            END,
        )
        .expect("planned")
        .expect("a statement")
    }

    /// §0's request.
    fn q1() -> Read {
        render(
            "profile_series",
            Method::LabelValues { name: "env" },
            &[r#"{service_name="checkout"}"#],
        )
    }

    /// Every Q2 case: (name, rendered, expected), single node and `_dist`.
    fn q2() -> Vec<(String, Read, Read)> {
        let names = ["__profile_type__".to_string(), "service_name".to_string()];
        let five = [
            "service_name".to_string(),
            "__name__".to_string(),
            "pyroscope_app".to_string(),
            "__type__".to_string(),
            "__profile_type__".to_string(),
        ];
        let checkout = [r#"{service_name="checkout"}"#];
        let mut out = Vec::new();
        for suffix in ["", "_dist"] {
            let table = format!("profile_series{suffix}");
            let from = format!("FROM {table}");
            let with = |select: &str, extra: &str, order: &str| {
                format!("{select}\n{from}\n{WHERE}{extra}\nORDER BY {order}")
            };
            let service = "\n  AND service = 'checkout'";
            let cases = [
                (
                    "S1",
                    render(&table, Method::ProfileTypes, &checkout),
                    Read::Strings(with("SELECT DISTINCT type_id AS s", "", "s")),
                ),
                (
                    "S2 legacy",
                    render(&table, Method::LabelNames { utf8: false }, &checkout),
                    Read::Strings(with(
                        &format!(
                            "SELECT DISTINCT arrayJoin(arrayFilter(n -> match(n, {LEGACY}), \
                             JSONExtractKeys(labels))) AS s"
                        ),
                        service,
                        "s",
                    )),
                ),
                (
                    "S2 utf8",
                    render(&table, Method::LabelNames { utf8: true }, &checkout),
                    Read::Strings(with(
                        "SELECT DISTINCT arrayJoin(JSONExtractKeys(labels)) AS s",
                        service,
                        "s",
                    )),
                ),
                (
                    "S4 names",
                    render(
                        &table,
                        Method::Series {
                            label_names: &names,
                            utf8: false,
                        },
                        &checkout,
                    ),
                    Read::Series {
                        sql: with(
                            "SELECT DISTINCT arrayFilter(kv -> has(['__profile_type__', \
                             'service_name'], kv.1), JSONExtractKeysAndValues(labels, 'String')) \
                             AS ls",
                            service,
                            "ls",
                        ),
                        compat: false,
                    },
                ),
                (
                    "S4 legacy",
                    render(
                        &table,
                        Method::Series {
                            label_names: &[],
                            utf8: false,
                        },
                        &checkout,
                    ),
                    Read::Series {
                        sql: with(
                            &format!(
                                "SELECT DISTINCT arrayFilter(kv -> match(kv.1, {LEGACY}), \
                                 JSONExtractKeysAndValues(labels, 'String')) AS ls"
                            ),
                            service,
                            "ls",
                        ),
                        compat: false,
                    },
                ),
                (
                    "S4 utf8",
                    render(
                        &table,
                        Method::Series {
                            label_names: &[],
                            utf8: true,
                        },
                        &checkout,
                    ),
                    Read::Series {
                        sql: with(
                            "SELECT DISTINCT JSONExtractKeysAndValues(labels, 'String') AS ls",
                            service,
                            "ls",
                        ),
                        compat: false,
                    },
                ),
                (
                    "S4 five names (D9)",
                    render(
                        &table,
                        Method::Series {
                            label_names: &five,
                            utf8: false,
                        },
                        &["{}"],
                    ),
                    Read::Series {
                        sql: with(
                            "SELECT DISTINCT arrayFilter(kv -> has(['__profile_type__', \
                             'service_name'], kv.1), JSONExtractKeysAndValues(labels, 'String')) \
                             AS ls",
                            "",
                            "ls",
                        ),
                        compat: true,
                    },
                ),
            ];
            for (name, got, want) in cases {
                out.push((format!("{name} {table}"), got, want));
            }
        }
        out
    }

    /// Q1: §0's request renders §0's statement, byte for byte.
    #[test]
    fn q1_the_example_request_renders_the_example_statement() {
        assert_eq!(
            q1(),
            Read::Strings(
                "SELECT DISTINCT JSONExtractString(labels, 'env') AS s
FROM profile_series
WHERE org_id = 't1' AND day BETWEEN '2026-11-15' AND '2026-11-15'
  AND bitAnd(hours, multiIf(day = '2026-11-15' AND day = '2026-11-15', 4096, day = '2026-11-15', 16773120, day = '2026-11-15', 8191, 16777215)) != 0
  AND service = 'checkout'
  AND s != ''
ORDER BY s"
                    .to_string()
            )
        );
    }

    /// Q2: S1, S2 and S4's forms, on both table names.
    #[test]
    fn q2_each_statement_form_renders_its_golden() {
        for (name, got, want) in q2() {
            assert_eq!(got, want, "{name}");
        }
    }

    /// Q3: the tenant check accepts every rendered text, and each reads the
    /// series table.
    #[test]
    fn q3_the_tenant_check_accepts_every_rendered_text() {
        let mut texts = vec![("Q1".to_string(), "".to_string(), q1())];
        texts.extend(q2().into_iter().map(|(name, got, _)| {
            let dist = if name.ends_with("_dist") { "_dist" } else { "" };
            (name, dist.to_string(), got)
        }));
        for (name, dist, read) in texts {
            let sql = read.sql();
            assert!(
                sql.contains(&format!("FROM profile_series{dist}\n")),
                "{name}: {sql}"
            );
            assert_eq!(
                tenant_scope::check(sql, &TenantTerm::of(&t1()), &tenant_scope::tables(&dist)),
                Ok(()),
                "{name}: {sql}"
            );
        }
    }
}
