//! Stub.

use pulsus_model::Tenant;

use super::engine::ProfilesReadError;
use super::selector::WindowLimits;

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

/// A catalogue request.
#[derive(Debug, Clone, Copy)]
pub struct CatalogueRequest<'a> {
    pub method: Method<'a>,
    pub matchers: &'a [&'a str],
    pub start_ms: i64,
    pub end_ms: i64,
}

/// The one statement a request sends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Read {
    Strings(String),
    Series { sql: String, compat: bool },
}

impl Read {
    pub fn sql(&self) -> &str {
        match self {
            Read::Strings(sql) | Read::Series { sql, .. } => sql,
        }
    }
}

pub fn plan(
    _tenant: &Tenant,
    _series_table: &str,
    _limits: WindowLimits,
    _req: &CatalogueRequest<'_>,
    _now_ms: i64,
) -> Result<Option<Read>, ProfilesReadError> {
    Ok(Some(Read::Strings(String::new())))
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
