//! The setup form (issue #746 part C): `GET /admin/setup` shows it,
//! `POST /admin/setup` answers with each refusal beside its field or with
//! the generated files, and `POST /admin/setup/file` returns one file as
//! an attachment. The form's fields go through `pulsus_setup::Inputs::from_pairs`
//! and one `validate`, as the JSON API's do.

use std::fmt::Write;

use axum::Form;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use pulsus_setup::{FieldError, FieldWarning, Inputs, Validated};

use super::html::{html_text, layout};

type Pairs = Vec<(String, String)>;

pub(crate) async fn form() -> Html<String> {
    let inputs = Inputs::from_pairs(starting_values());
    Html(page(&inputs, &[], None))
}

pub(crate) async fn submit(Form(pairs): Form<Pairs>) -> Html<String> {
    let inputs = Inputs::from_pairs(pairs);
    match pulsus_setup::validate(&inputs) {
        Ok(v) => Html(page(&inputs, &[], Some(&v))),
        Err(errors) => Html(page(&inputs, &errors, None)),
    }
}

pub(crate) async fn file(Form(pairs): Form<Pairs>) -> Response {
    let wanted = pairs
        .iter()
        .find(|(k, _)| k == "file")
        .map(|(_, v)| v.clone())
        .unwrap_or_default();
    let inputs = Inputs::from_pairs(pairs.into_iter().filter(|(k, _)| k != "file"));
    let v = match pulsus_setup::validate(&inputs) {
        Ok(v) => v,
        Err(errors) => {
            let text: String = errors
                .iter()
                .map(|e| format!("{}: {}\n", e.field, e.message))
                .collect();
            return (StatusCode::UNPROCESSABLE_ENTITY, text).into_response();
        }
    };
    let Some(found) = pulsus_setup::bundle(&v)
        .into_iter()
        .find(|f| f.path == wanted)
    else {
        return (StatusCode::NOT_FOUND, "no such file").into_response();
    };
    let mut res = found.content.into_response();
    // Paths are digits, letters, `.`, `_`, `-` and `/`.
    let name = found.path.replace('/', "_");
    if let Ok(value) = HeaderValue::from_str(&format!("attachment; filename=\"{name}\"")) {
        res.headers_mut().insert(header::CONTENT_DISPOSITION, value);
    }
    res
}

/// The form's starting values: a working example, so Generate at once
/// shows a result. Six hosts in two shards of three, three Keeper hosts,
/// object storage, and the default levels with their default switch
/// points, each kept 30 days on local disk and longer on object storage.
fn starting_values() -> Vec<(String, String)> {
    let mut pairs = pulsus_setup::prefilled();
    let example = [
        ("hosts", "ch1\nch2\nch3\nch4\nch5\nch6"),
        ("keepers", "kp1\nkp2\nkp3"),
        ("shards", "2"),
        ("replicas", "3"),
        ("remote_url", "https://bucket.example/pulsus/"),
        ("cores", "32"),
        ("memory_gib", "128"),
        ("disk_gib", "2048"),
        ("raw_local_days", "7"),
        ("raw_remote_days", "23"),
        ("l1_local", "30"),
        ("l1_remote", "60"),
        ("l2_local", "30"),
        ("l2_remote", "335"),
        ("l3_local", "30"),
        ("l3_remote", "700"),
        ("l4_local", "30"),
        ("l4_remote", "1795"),
    ];
    for (name, value) in example {
        pairs.retain(|(n, _)| n != name);
        pairs.push((name.to_string(), value.to_string()));
    }
    pairs
}

/// An absolute path of `[A-Za-z0-9._/-]` (W5, W7), as an HTML `pattern`.
const PATH_PATTERN: &str = r"\/[A-Za-z0-9._\/\-]*";
/// W5: the same, ending in `/`.
const DIR_PATTERN: &str = r"\/([A-Za-z0-9._\/\-]*\/)?";
/// W6, as far as a pattern can: `http` or `https`, a host, a path ending in
/// `/`, no `?`, `#` or `@`. The server's URL parse is the full rule.
const URL_PATTERN: &str = r"https?:\/\/[^\/?#@\s]+\/([^?#@\s]*\/)?";
/// W13 and the schema's name rule: letters, digits and `_`, starting with a
/// letter or `_`.
const CLUSTER_PATTERN: &str = "[A-Za-z_][A-Za-z0-9_]*";
/// The schema's name rule: letters, digits and `_`; blank is `pulsus`.
const DATABASE_PATTERN: &str = "[A-Za-z0-9_]*";
const DAYS_MAX: &str = "4294967295";

/// How a field is entered, and the browser's checks: each the server's.
enum Kind {
    /// One value per line.
    Lines,
    /// Text, with a `pattern` and an example `placeholder`.
    Text {
        pattern: &'static str,
        placeholder: &'static str,
    },
    /// A whole number from `min` to `max`.
    Number {
        min: &'static str,
        max: &'static str,
    },
}

struct Field {
    name: &'static str,
    label: &'static str,
    required: bool,
    kind: Kind,
    /// What it is, an example, and the rule.
    help: &'static str,
}

const HOSTS: Field = Field {
    name: "hosts",
    label: "ClickHouse hosts",
    required: true,
    kind: Kind::Lines,
    help: "One ClickHouse server per line, each an IPv4 address or a host name (IPv6 is not accepted), \
           for example ch1.example.com. List each shard's replicas together: with 3 replicas, \
           lines 1-3 are shard 1 and lines 4-6 shard 2. Give exactly shards × replicas hosts, \
           at most 100, none twice.",
};
const KEEPERS: Field = Field {
    name: "keepers",
    label: "Keeper hosts",
    required: true,
    kind: Kind::Lines,
    help: "One ClickHouse Keeper host per line, an IPv4 address or a host name, for example \
           kp1.example.com. 1 host, or from 3 to 9, of their own: none may also be a ClickHouse \
           host. Keeper commits on a majority, so 1 host survives no failure, nor do 2, which are \
           refused; 3 survive one failure, as do 4; 5 survive two.",
};
const CLUSTER: Field = Field {
    name: "cluster",
    label: "Cluster name",
    required: true,
    kind: Kind::Text {
        pattern: CLUSTER_PATTERN,
        placeholder: "pulsus_cluster",
    },
    help: "The ClickHouse cluster's name, used in every server's config and by the schema, \
           for example pulsus_cluster. Letters, digits and _, starting with a letter or _.",
};
const SHARDS: Field = Field {
    name: "shards",
    label: "Shards",
    required: true,
    kind: Kind::Number {
        min: "1",
        max: "100",
    },
    help: "How many parts the data is split into, for example 2. \
           A whole number from 1 to 100; shards × replicas is at most 100.",
};
const REPLICAS: Field = Field {
    name: "replicas",
    label: "Replicas per shard",
    required: true,
    kind: Kind::Number {
        min: "1",
        max: "100",
    },
    help: "How many copies of each shard, each on its own host, for example 3. \
           A whole number from 1 to 100.",
};
const TLS: [Field; 3] = [
    Field {
        name: "tls_cert",
        label: "Certificate file",
        required: false,
        kind: Kind::Text {
            pattern: PATH_PATTERN,
            placeholder: "/etc/clickhouse-server/certs/server.crt",
        },
        help: "The server certificate, PEM. An absolute path of letters, digits, . _ / -.",
    },
    Field {
        name: "tls_key",
        label: "Key file",
        required: false,
        kind: Kind::Text {
            pattern: PATH_PATTERN,
            placeholder: "/etc/clickhouse-server/certs/server.key",
        },
        help: "The certificate's private key, PEM. An absolute path of letters, digits, . _ / -.",
    },
    Field {
        name: "tls_ca",
        label: "CA file",
        required: false,
        kind: Kind::Text {
            pattern: PATH_PATTERN,
            placeholder: "/etc/clickhouse-server/certs/ca.crt",
        },
        help: "The certificate authority every host trusts, PEM. \
               An absolute path of letters, digits, . _ / -.",
    },
];
const LOCAL_PATH: Field = Field {
    name: "local_path",
    label: "Local storage path",
    required: true,
    kind: Kind::Text {
        pattern: DIR_PATTERN,
        placeholder: "/var/lib/clickhouse/",
    },
    help: "Where ClickHouse keeps its data on each host, for example /var/lib/clickhouse/. \
           An absolute directory ending in /, of letters, digits, . _ / -.",
};
const REMOTE_URL: Field = Field {
    name: "remote_url",
    label: "Object storage URL",
    required: false,
    kind: Kind::Text {
        pattern: URL_PATTERN,
        placeholder: "https://bucket.s3.eu-west-1.amazonaws.com/pulsus/",
    },
    help: "Optional: an S3-compatible bucket that older data moves to, for example \
           https://bucket.s3.eu-west-1.amazonaws.com/pulsus/. An http or https URL with a host and, \
           if given, a port from 1 to 65535, ending in /, with no user, query or fragment. Each host writes under \
           URL/shard/replica/. Leave blank for local disk only; then every \
           \"days on object storage\" must be blank or 0. The access key and secret are not \
           entered here: give them to each ClickHouse server as the environment variables \
           PULSUS_S3_ACCESS_KEY_ID and PULSUS_S3_SECRET_ACCESS_KEY (see step 2 of setup.md).",
};
const HARDWARE: [Field; 3] = [
    Field {
        name: "cores",
        label: "CPU cores",
        required: true,
        kind: Kind::Number {
            min: "1",
            max: "1000000",
        },
        help: "Hardware threads on each host, for example 32. A whole number from 1 to 1000000.",
    },
    Field {
        name: "memory_gib",
        label: "Memory (GiB)",
        required: true,
        kind: Kind::Number {
            min: "1",
            max: "1000000",
        },
        help: "Memory on each host, for example 128. A whole number from 1 to 1000000.",
    },
    Field {
        name: "disk_gib",
        label: "Disk (GiB)",
        required: true,
        kind: Kind::Number {
            min: "1",
            max: "1000000",
        },
        help: "Local disk on each host, for example 2048. A whole number from 1 to 1000000.",
    },
];
const DATABASE: Field = Field {
    name: "database",
    label: "Database name",
    required: false,
    kind: Kind::Text {
        pattern: DATABASE_PATTERN,
        placeholder: "pulsus",
    },
    help: "The ClickHouse database PulsusDB's tables live in, for example pulsus. \
           Letters, digits and _; blank is pulsus.",
};
const RAW: [Field; 2] = [
    Field {
        name: "raw_local_days",
        label: "Days on local disk",
        required: true,
        kind: Kind::Number {
            min: "1",
            max: DAYS_MAX,
        },
        help: "How long raw samples, logs and spans stay on local disk, for example 7. \
               A whole number from 1 to 4294967295.",
    },
    Field {
        name: "raw_remote_days",
        label: "Then days on object storage",
        required: false,
        kind: Kind::Number {
            min: "0",
            max: DAYS_MAX,
        },
        help: "How long they stay on object storage after that, for example 23; blank is 0. \
               A whole number from 0 to 4294967295; above 0 needs an object storage URL. \
               Raw data is kept for the two added together.",
    },
];

/// The page's name for a refused field or group, and the id it links to.
fn field_label(field: &str) -> (String, String) {
    let all = [
        &HOSTS,
        &KEEPERS,
        &CLUSTER,
        &SHARDS,
        &REPLICAS,
        &LOCAL_PATH,
        &REMOTE_URL,
        &DATABASE,
    ]
    .into_iter()
    .chain(&TLS)
    .chain(&HARDWARE)
    .chain(&RAW);
    for f in all {
        if f.name == field {
            let part = if f.name.starts_with("raw_") {
                "Raw data: "
            } else {
                ""
            };
            return (format!("{part}{}", f.label), format!("f-{field}"));
        }
    }
    let group = match field {
        "tls" => Some(("Security (TLS)", "g-tls")),
        "raw" => Some(("Raw data retention", "g-raw")),
        "levels" => Some(("Rollup levels", "g-levels")),
        "schema" => Some(("Schema", "part-2")),
        _ => None,
    };
    if let Some((label, id)) = group {
        return (label.to_string(), id.to_string());
    }
    if let Some((k, column)) = field
        .strip_prefix('l')
        .and_then(|rest| rest.split_once('_'))
    {
        let column = match column {
            "width" => "resolution",
            "from" => "range",
            "local" => "days on local disk",
            "remote" => "days on object storage",
            other => other,
        };
        return (format!("Level {k} {column}"), format!("f-{field}"));
    }
    (field.to_string(), format!("f-{field}"))
}

/// The messages refused against `field`, each under it.
fn messages(errors: &[FieldError], field: &str) -> String {
    errors
        .iter()
        .filter(|e| e.field == field)
        .map(|e| format!("<p class=\"field-error\">{}</p>", html_text(&e.message)))
        .collect()
}

fn refused(errors: &[FieldError], field: &str) -> bool {
    errors.iter().any(|e| e.field == field)
}

/// The control alone: its name, value and the browser's checks.
fn control(
    inputs: &Inputs,
    errors: &[FieldError],
    name: &str,
    label: &str,
    kind: &Kind,
    required: bool,
    help_id: Option<&str>,
) -> String {
    let value = html_text(inputs.get(name));
    let mut attrs = format!("id=\"f-{name}\" name=\"{name}\"");
    if refused(errors, name) {
        attrs.push_str(" class=\"invalid\" aria-invalid=\"true\"");
    }
    if required {
        attrs.push_str(" required");
    }
    if let Some(id) = help_id {
        let _ = write!(attrs, " aria-describedby=\"{id}\"");
    } else {
        let _ = write!(attrs, " aria-label=\"{}\"", html_text(label));
    }
    match kind {
        Kind::Lines => {
            format!("<textarea {attrs} rows=\"6\" spellcheck=\"false\">{value}</textarea>")
        }
        Kind::Text {
            pattern,
            placeholder,
        } => format!(
            "<input {attrs} type=\"text\" value=\"{value}\" pattern=\"{}\" placeholder=\"{}\" spellcheck=\"false\">",
            html_text(pattern),
            html_text(placeholder)
        ),
        Kind::Number { min, max } => format!(
            "<input {attrs} type=\"number\" value=\"{value}\" min=\"{min}\" max=\"{max}\" step=\"1\" inputmode=\"numeric\">"
        ),
    }
}

/// The warnings on `field`, each under it.
fn warning_messages(warnings: &[FieldWarning], field: &str) -> String {
    warnings
        .iter()
        .filter(|w| w.field == field)
        .map(|w| format!("<p class=\"field-warning\">{}</p>", html_text(&w.message)))
        .collect()
}

/// One field: its label, control, help, every message refused against
/// it, and every warning on it.
fn field(inputs: &Inputs, errors: &[FieldError], warnings: &[FieldWarning], f: &Field) -> String {
    let help_id = format!("h-{}", f.name);
    let required = if f.required {
        " <span class=\"req\" aria-hidden=\"true\">*</span>"
    } else {
        " <span class=\"opt\">optional</span>"
    };
    format!(
        "<div class=\"field\"><label for=\"f-{name}\">{label}{required}</label>{control}\
         <p class=\"help\" id=\"{help_id}\">{help}</p>{messages}{warned}</div>\n",
        name = f.name,
        label = html_text(f.label),
        control = control(
            inputs,
            errors,
            f.name,
            f.label,
            &f.kind,
            f.required,
            Some(&help_id)
        ),
        help = html_text(f.help),
        messages = messages(errors, f.name),
        warned = warning_messages(warnings, f.name),
    )
}

/// A group's own refusals (`tls`, `raw`, `levels`, `schema`), above it.
fn group_messages(errors: &[FieldError], group: &str) -> String {
    let text = messages(errors, group);
    if text.is_empty() {
        text
    } else {
        format!("<div class=\"group-error\">{text}</div>\n")
    }
}

fn subsection(id: &str, title: &str, note: &str) -> String {
    format!(
        "<fieldset id=\"{id}\"><legend>{}</legend><p class=\"note\">{}</p>\n",
        html_text(title),
        html_text(note)
    )
}

const INTRO: &str = "<section class=\"intro\"><p>This form generates everything a new \
    PulsusDB cluster needs: each ClickHouse server's config, each ClickHouse Keeper \
    node's config, the PulsusDB schema, PulsusDB's own settings, and step-by-step \
    setup instructions.</p><p>Fill in <a href=\"#part-1\">part 1, the ClickHouse cluster</a>, \
    then <a href=\"#part-2\">part 2, the PulsusDB schema</a>, and press <strong>Generate</strong>. \
    Every field is checked when you press it, and anything refused is listed at the top \
    and marked beside its field.</p><p>The form starts filled in with a working example \
    (six hosts in two shards of three replicas, three Keeper hosts): press Generate now \
    to see what it produces, then change it to your own hosts.</p></section>\n";

/// The form with `inputs`' values, `errors` listed and under their
/// fields, and `result`'s lines and files when every rule accepted.
fn page(inputs: &Inputs, errors: &[FieldError], result: Option<&Validated>) -> String {
    let warnings = result.map(|v| v.warnings.as_slice()).unwrap_or_default();
    let mut body = String::from(INTRO);
    if !errors.is_empty() {
        let count = if errors.len() == 1 {
            "1 problem".to_string()
        } else {
            format!("{} problems", errors.len())
        };
        let _ = write!(
            body,
            "<section class=\"summary bad\" role=\"alert\"><h2>Nothing was generated: {count} to fix</h2>\
             <p>Each is also shown in red under its field below. Your values are kept.</p>\
             <ul class=\"errors\">"
        );
        for e in errors {
            let (label, id) = field_label(&e.field);
            let _ = write!(
                body,
                "<li data-field=\"{}\"><a href=\"#{}\">{}</a>: <span class=\"msg\">{}</span></li>",
                html_text(&e.field),
                html_text(&id),
                html_text(&label),
                html_text(&e.message)
            );
        }
        body.push_str("</ul></section>\n");
    }
    if let Some(v) = result {
        body.push_str(&result_html(inputs, v));
    }

    body.push_str("<form id=\"inputs\" method=\"post\" action=\"/admin/setup\">\n");

    // Part 1.
    body.push_str(
        "<section class=\"card part\" id=\"part-1\"><header><span class=\"step\">1</span>\
         <div><h2>The ClickHouse cluster</h2><p class=\"note\">Produces each ClickHouse \
         server's config and each Keeper node's config.</p></div></header>\n",
    );
    body.push_str(&subsection(
        "g-topology",
        "Hosts and topology",
        "Which machines run ClickHouse and Keeper, and how the data is split and copied across them.",
    ));
    body.push_str("<div class=\"row cols-3\">");
    body.push_str(&field(inputs, errors, warnings, &SHARDS));
    body.push_str(&field(inputs, errors, warnings, &REPLICAS));
    body.push_str(&field(inputs, errors, warnings, &CLUSTER));
    body.push_str("</div><div class=\"row cols-2\">");
    body.push_str(&field(inputs, errors, warnings, &HOSTS));
    body.push_str(&field(inputs, errors, warnings, &KEEPERS));
    body.push_str("</div></fieldset>\n");

    body.push_str(&subsection(
        "g-tls",
        "Security (TLS)",
        "Optional. The same three file paths on every ClickHouse and Keeper host. Give all three, \
         or leave all three blank for no TLS. With them, every plaintext port is closed.",
    ));
    body.push_str(&group_messages(errors, "tls"));
    body.push_str("<div class=\"row cols-3\">");
    for f in &TLS {
        body.push_str(&field(inputs, errors, warnings, f));
    }
    body.push_str("</div></fieldset>\n");

    body.push_str(&subsection(
        "g-storage",
        "Storage",
        "Where each host keeps its data, and the object storage older data can move to.",
    ));
    body.push_str("<div class=\"row cols-2\">");
    body.push_str(&field(inputs, errors, warnings, &LOCAL_PATH));
    body.push_str(&field(inputs, errors, warnings, &REMOTE_URL));
    body.push_str("</div></fieldset>\n");

    body.push_str(&subsection(
        "g-hardware",
        "Hardware",
        "Each host's size. Shown with the result as sizing advice; not written into any file, \
         because the server derives its memory and thread settings itself.",
    ));
    body.push_str("<div class=\"row cols-3\">");
    for f in &HARDWARE {
        body.push_str(&field(inputs, errors, warnings, f));
    }
    body.push_str("</div></fieldset>\n</section>\n");

    // Part 2.
    body.push_str(
        "<section class=\"card part\" id=\"part-2\"><header><span class=\"step\">2</span>\
         <div><h2>The PulsusDB schema</h2><p class=\"note\">Produces the schema, with how long \
         each kind of data is kept, and PulsusDB's settings.</p></div></header>\n",
    );
    body.push_str(&group_messages(errors, "schema"));
    body.push_str("<div class=\"row cols-3\">");
    body.push_str(&field(inputs, errors, warnings, &DATABASE));
    body.push_str("</div>\n");
    body.push_str(&subsection(
        "g-raw",
        "Raw data retention",
        "Raw metric samples, log lines and trace spans: days on local disk first, then days on \
         object storage.",
    ));
    body.push_str(&group_messages(errors, "raw"));
    body.push_str("<div class=\"row cols-2\">");
    for f in &RAW {
        body.push_str(&field(inputs, errors, warnings, f));
    }
    body.push_str("</div></fieldset>\n");

    body.push_str(&subsection(
        "g-levels",
        "Rollup levels",
        "Metrics are also stored pre-aggregated at coarser resolutions, so long-range queries \
         read far fewer rows. One row per level, up to eight, smallest resolution first; \
         leave the resolution blank for no level, with no gaps between rows.",
    ));
    body.push_str(&group_messages(errors, "levels"));
    body.push_str(
        "<div class=\"table-wrap\"><table class=\"levels\"><thead><tr><th scope=\"col\">Level</th>\
         <th scope=\"col\">Resolution (s)</th><th scope=\"col\">Range from (s)</th>\
         <th scope=\"col\">Days on local disk</th><th scope=\"col\">Then days on object storage</th>\
         </tr></thead><tbody>\n",
    );
    let width = Kind::Number {
        min: "1",
        max: "86400",
    };
    let from = Kind::Number {
        min: "1",
        max: "999999999",
    };
    let local = Kind::Number {
        min: "1",
        max: DAYS_MAX,
    };
    let remote = Kind::Number {
        min: "0",
        max: DAYS_MAX,
    };
    for k in 1..=pulsus_schema::MAX_LEVELS {
        let names = [
            format!("l{k}_width"),
            format!("l{k}_from"),
            format!("l{k}_local"),
            format!("l{k}_remote"),
        ];
        let cell = |i: usize, kind: &Kind, column: &str| {
            let name = &names[i];
            format!(
                "<td>{}</td>",
                control(
                    inputs,
                    errors,
                    name,
                    &format!("Level {k} {column}"),
                    kind,
                    false,
                    None
                )
            )
        };
        let _ = writeln!(
            body,
            "<tr><th scope=\"row\">{k}</th>{}{}{}{}</tr>",
            cell(0, &width, "resolution"),
            cell(1, &from, "range from"),
            cell(2, &local, "days on local disk"),
            cell(3, &remote, "days on object storage")
        );
        let row_messages: String = names
            .iter()
            .filter(|name| refused(errors, name))
            .map(|name| {
                let (label, _) = field_label(name);
                errors
                    .iter()
                    .filter(|e| &e.field == name)
                    .map(|e| {
                        format!(
                            "<p class=\"field-error\">{}: {}</p>",
                            html_text(&label),
                            html_text(&e.message)
                        )
                    })
                    .collect::<String>()
            })
            .collect();
        if !row_messages.is_empty() {
            let _ = writeln!(
                body,
                "<tr class=\"row-errors\"><td colspan=\"5\">{row_messages}</td></tr>"
            );
        }
    }
    body.push_str("</tbody></table></div>\n");
    body.push_str(
        "<dl class=\"column-help\">\
         <dt>Resolution</dt><dd>Seconds per stored point, a whole number from 1 to 86400 that \
         divides a day (86400); below 60 it must divide 60. Each level's is larger than the one \
         above and a multiple of it. Example: 60, 720, 7200, 86400.</dd>\
         <dt>Range from</dt><dd>The query range, in seconds, from which a range query reads \
         this level; a range shorter than level 1's reads the raw samples. A whole number from \
         1 to 999999999, each larger than the row above and at least its own resolution. \
         Example: 10800 (3 hours), 57600 (16 hours), 604800 (7 days), 7776000 (90 days).</dd>\
         <dt>Days on local disk</dt><dd>A whole number from 1 to 4294967295.</dd>\
         <dt>Then days on object storage</dt><dd>Blank or 0 keeps the level on local disk; \
         above 0 needs an object storage URL. A whole number from 0 to 4294967295.</dd>\
         <dt>Kept in all</dt><dd>Local plus object storage days. Each level is kept at least \
         as long as raw data, and each level but the last at least ⌈the next level's range \
         ÷ 86400⌉ + 1 days, so a query that switches to the next level finds every day it \
         needs: 2, 8 and 91 days with the example ranges.</dd></dl>\n",
    );
    body.push_str("</fieldset>\n</section>\n");
    body.push_str(
        "<div class=\"actions\"><button type=\"submit\" class=\"primary\">Generate</button>\
         <span class=\"help\">Nothing is saved or applied: Generate only shows the files.</span></div>\n</form>\n",
    );
    layout("Setup", &body)
}

/// The group heading a bundle path is listed under.
fn file_group(path: &str, v: &Validated) -> String {
    let host = |rest: &str, hosts: &[String]| {
        rest.split('/')
            .next()
            .and_then(|n| n.parse::<usize>().ok())
            .map(|n| (n, hosts.get(n - 1).cloned().unwrap_or_default()))
    };
    if let Some((n, name)) = path
        .strip_prefix("nodes/")
        .and_then(|rest| host(rest, &v.hosts))
    {
        return format!("ClickHouse server {n}: {name}, shard {}", v.shard_of(n));
    }
    if let Some((n, name)) = path
        .strip_prefix("keepers/")
        .and_then(|rest| host(rest, &v.keepers))
    {
        return format!("Keeper node {n}: {name}");
    }
    match path {
        "pulsusdb.env" => "PulsusDB settings".to_string(),
        "setup.md" => "Setup instructions".to_string(),
        _ => "PulsusDB schema".to_string(),
    }
}

/// What the schema keeps, the sizing, and every file with its contents and
/// its download button.
fn result_html(inputs: &Inputs, v: &Validated) -> String {
    let files = pulsus_setup::bundle(v);
    let mut out = format!(
        "<section class=\"summary good\" id=\"result\"><h2>Generated {} files</h2>\
         <p>For {} ClickHouse hosts in {} shards of {} replicas, and {}. \
         Start with <a href=\"#file-setup.md\">setup.md</a>: it walks through every step.</p>\
         <h3>What the schema keeps</h3><ul class=\"totals\">",
        files.len(),
        v.hosts.len(),
        v.shards,
        v.replicas,
        pulsus_setup::keeper_hosts(v)
    );
    for line in pulsus_setup::total_lines(v) {
        let _ = write!(out, "<li>{}</li>", html_text(&line));
    }
    out.push_str("</ul>\n<h3>Sizing, not written to any file</h3>\n<dl>");
    for (label, value) in pulsus_setup::sizing(v) {
        let _ = write!(
            out,
            "<dt>{}</dt><dd>{}</dd>",
            html_text(label),
            html_text(&value)
        );
    }
    out.push_str(
        "</dl><p><a href=\"#inputs\">Change the inputs</a></p></section>\n\
         <form class=\"files\" method=\"post\" action=\"/admin/setup/file\">\n",
    );
    for name in pulsus_setup::field_names() {
        let _ = writeln!(
            out,
            "<input type=\"hidden\" name=\"{name}\" value=\"{}\">",
            html_text(inputs.get(&name))
        );
    }
    let mut group = String::new();
    for f in &files {
        let this = file_group(&f.path, v);
        if this != group {
            let _ = writeln!(out, "<h3 class=\"file-group\">{}</h3>", html_text(&this));
            group = this;
        }
        let path = html_text(&f.path);
        let open = if f.path == "setup.md" { " open" } else { "" };
        let lines = f.content.lines().count();
        let dest = html_text(&pulsus_setup::destination_label(v, &f.path));
        let _ = writeln!(
            out,
            "<article class=\"card file\" id=\"file-{path}\"><header><div class=\"where\">\
             <span class=\"dest\">{dest}</span>\
             <span class=\"in-bundle\">in the bundle: {path}</span></div>\
             <span class=\"size\">{lines} lines</span>\
             <button type=\"submit\" name=\"file\" value=\"{path}\">Download</button></header>\
             <details{open}><summary>Show contents</summary><pre>{}</pre></details></article>",
            html_text(&f.content)
        );
    }
    out.push_str("</form>\n");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The form starts with a working example: every rule accepts it.
    #[test]
    fn the_starting_values_are_accepted() {
        let inputs = Inputs::from_pairs(starting_values());
        let v = match pulsus_setup::validate(&inputs) {
            Ok(v) => v,
            Err(errors) => panic!("{errors:?}"),
        };
        assert_eq!(v.hosts.len(), 6);
        assert_eq!((v.shards, v.replicas), (2, 3));
        assert_eq!(v.keepers.len(), 3);
        let widths: Vec<&str> = v.levels.iter().map(|l| l.width.as_str()).collect();
        assert_eq!(widths, ["60", "720", "7200", "86400"]);
        let froms: Vec<&str> = v.levels.iter().map(|l| l.from.as_str()).collect();
        assert_eq!(froms, ["10800", "57600", "604800", "7776000"]);
        assert_eq!(
            v.vars["PULSUS_ROLLUP_RANGES"], "10800,57600,604800,7776000",
            "level 1 carries its own range"
        );
    }

    /// Issue #746: level 1's "Range from" is a field like every other
    /// row's, starting at the default 3600, and refused like them: below
    /// its resolution, the message is the schema's, on the level group.
    #[test]
    fn level_one_has_its_own_range_from() {
        let page = page(&Inputs::from_pairs(starting_values()), &[], None);
        assert!(
            page.contains(" name=\"l1_from\""),
            "level 1 has a Range from field: {page}"
        );
        assert!(
            page.contains("aria-label=\"Level 1 range from\""),
            "the field is labelled: {page}"
        );
        assert!(!page.contains("0, every range"), "{page}");
        let mut pairs = starting_values();
        pairs.retain(|(n, _)| n != "l1_from");
        pairs.push(("l1_from".to_string(), "30".to_string()));
        let errors = pulsus_setup::validate(&Inputs::from_pairs(pairs)).expect_err("refused");
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert_eq!(errors[0].field, "levels");
        assert_eq!(
            errors[0].message,
            "PULSUS_ROLLUP_RANGES: 30 is shorter than its level 60"
        );
    }

    /// The object storage URL's help says where the access key and secret
    /// go: not on the form, but in each server's environment.
    #[test]
    fn the_remote_url_help_says_where_the_s3_key_goes() {
        let page = page(&Inputs::from_pairs(starting_values()), &[], None);
        let help = html_text(
            "The access key and secret are not entered here: give them to each ClickHouse \
             server as the environment variables PULSUS_S3_ACCESS_KEY_ID and \
             PULSUS_S3_SECRET_ACCESS_KEY (see step 2 of setup.md).",
        );
        let at = page.find("id=\"h-remote_url\"").expect("the URL's help");
        let rest = &page[at..];
        let text = &rest[..rest.find("</p>").expect("the help closes")];
        assert!(text.contains(&help), "{text}");
    }

    /// Every field the server reads is on the form, under its own name.
    #[test]
    fn every_field_is_on_the_form() {
        let page = page(&Inputs::from_pairs(starting_values()), &[], None);
        for name in pulsus_setup::field_names() {
            assert_eq!(
                page.matches(&format!(" name=\"{name}\"")).count(),
                1,
                "{name}"
            );
        }
    }

    /// A refused field is outlined, keeps its value, shows its message
    /// under it, and is listed at the top with a link to it.
    #[test]
    fn a_refused_field_is_marked_and_keeps_its_value() {
        let mut pairs = starting_values();
        pairs.retain(|(n, _)| n != "shards");
        pairs.push(("shards".to_string(), "101".to_string()));
        let inputs = Inputs::from_pairs(pairs);
        let errors = pulsus_setup::validate(&inputs).expect_err("refused");
        let page = page(&inputs, &errors, None);
        assert!(
            page.contains(
                "id=\"f-shards\" name=\"shards\" class=\"invalid\" aria-invalid=\"true\" required"
            ),
            "{page}"
        );
        assert!(
            page.contains("value=\"101\" min=\"1\" max=\"100\""),
            "{page}"
        );
        assert!(
            page.contains(
                "<li data-field=\"shards\"><a href=\"#f-shards\">Shards</a>: \
                 <span class=\"msg\">must be a whole number from 1 to 100</span></li>"
            ),
            "{page}"
        );
        assert!(
            page.contains("<p class=\"field-error\">must be a whole number from 1 to 100</p>"),
            "{page}"
        );
        assert_eq!(page.matches("class=\"invalid\"").count(), 1, "{page}");
    }
}
