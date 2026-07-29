//! `template-audit` (issue #230, plan AC-22): the re-runnable
//! non-reproducibility audit over the template-engine printf domain —
//! the executable form of plan v7 §C, so a reference bump re-derives
//! the §D exclusion ledger instead of trusting prose.
//!
//! Procedure:
//! 1. Generate one `line_format` query per (shape × form) cell of the
//!    re-derived verb domain (24 argument-consuming forms × 9 value
//!    shapes) plus the `%%`/NOVERB/catch-all edge cells.
//! 2. Push one entry at ONE fixed absolute-ns timestamp into TWO fresh
//!    reference containers and run every query against both.
//! 3. Emit both evidence sets:
//!    - the cross-container DIFF (address-carrying cells whose pointee
//!      is per-process);
//!    - the ADDRESS-TOKEN SCAN (hex `0x…` runs), which also catches the
//!      package-global addresses a diff alone cannot see (§D items
//!      2/3 — identical across containers by construction).
//!
//! Offline-only, like the corpus capture: CI never runs it. Start two
//! stock containers first, e.g.
//! `podman run --rm -d -p 3199:3100 grafana/loki:<pinned>` and the same
//! on 3198, then:
//! `cargo run -p xtask -- template-audit \
//!    --url-a http://127.0.0.1:3199 --url-b http://127.0.0.1:3198`

use std::io::{Read, Write};

use anyhow::{Context, bail};
use serde::Deserialize;

#[derive(clap::Parser)]
pub struct TemplateAuditArgs {
    /// First reference container base URL.
    #[arg(long, default_value = "http://127.0.0.1:3199")]
    url_a: String,
    /// Second (fresh) reference container base URL.
    #[arg(long, default_value = "http://127.0.0.1:3198")]
    url_b: String,
    /// Write the full per-cell report here (JSON lines).
    #[arg(long)]
    out: Option<String>,
}

const FORMS: [&str; 24] = [
    "v", "+v", "#v", "T", "s", "q", "d", "b", "o", "O", "x", "X", "c", "U", "e", "E", "f", "F",
    "g", "G", "t", "p", "w", "y",
];

const SHAPES: [(&str, &str); 9] = [
    ("bytes", "$t.MarshalJSON"),
    ("time_stock", "$t"),
    ("time_utc", "$t.UTC"),
    ("time_loaded", "$paris"),
    ("duration", "$t.Sub ($t.AddDate 0 0 -1)"),
    ("month", "$t.Month"),
    ("weekday", "$t.Weekday"),
    ("loc_stock", "$t.Location"),
    ("loc_loaded", "$paris.Location"),
];

/// The `%%`/NOVERB/`%O`/`%w`/catch-all edge cells (plan v7 §E).
const EDGE_CELLS: [(&str, &str); 8] = [
    ("edge_percent", r#"{{ printf "%%" }}"#),
    ("edge_percent_mid", r#"{{ printf "a%%b" }}"#),
    ("edge_percent_tail", r#"{{ printf "100%%" }}"#),
    ("edge_percent_extra", r#"{{ printf "%%" .pad }}"#),
    ("edge_noverb", r#"{{ printf "abc%" }}"#),
    ("edge_catchall", r#"{{ printf "%y" .pad }}"#),
    ("edge_o", r#"{{ printf "%O|%O|%O" 8 255 0 }}"#),
    ("edge_w", r#"{{ printf "%w|%w" "Hello" 7 }}"#),
];

struct Cell {
    name: String,
    query_body: String,
}

fn cells() -> Vec<Cell> {
    let prelude = "{{ $t := __timestamp__ }}\
                   {{ $paris := toDateInZone \"2006-01-02\" \"Europe/Paris\" \"2023-01-15\" }}";
    let mut out = Vec::new();
    for (shape, expr) in SHAPES {
        for form in FORMS {
            out.push(Cell {
                name: format!("{shape}x{form}"),
                query_body: format!("{prelude}{shape}:{{{{ printf \"%{form}\" ({expr}) }}}}|"),
            });
        }
    }
    for (name, body) in EDGE_CELLS {
        out.push(Cell {
            name: name.to_string(),
            query_body: body.to_string(),
        });
    }
    out
}

#[derive(Deserialize)]
struct QueryResponse {
    data: QueryData,
}

#[derive(Deserialize)]
struct QueryData {
    result: Vec<StreamResult>,
}

#[derive(Deserialize)]
struct StreamResult {
    stream: std::collections::BTreeMap<String, String>,
    values: Vec<(String, String)>,
}

/// A minimal HTTP/1.1 client over `TcpStream` — the audit talks only to
/// local plaintext containers, so xtask stays free of an HTTP-client
/// dependency (KISS-testing convention).
fn http(
    base: &str,
    method: &str,
    path_and_query: &str,
    body: Option<&str>,
) -> anyhow::Result<(u16, String)> {
    let hostport = base
        .strip_prefix("http://")
        .with_context(|| format!("only http:// bases are supported, got {base}"))?
        .trim_end_matches('/');
    let mut stream =
        std::net::TcpStream::connect(hostport).with_context(|| format!("connect {hostport}"))?;
    let body = body.unwrap_or("");
    let req = format!(
        "{method} {path_and_query} HTTP/1.1\r\nHost: {hostport}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(req.as_bytes())?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw)?;
    let text = String::from_utf8_lossy(&raw);
    let mut parts = text.splitn(2, "\r\n\r\n");
    let head = parts.next().unwrap_or_default();
    let payload = parts.next().unwrap_or_default();
    let status: u16 = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .context("malformed HTTP status line")?;
    // Undo chunked transfer-encoding when present.
    let payload = if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        dechunk(payload)
    } else {
        payload.to_string()
    };
    Ok((status, payload))
}

fn dechunk(s: &str) -> String {
    let mut out = String::new();
    let mut rest = s;
    loop {
        let Some((size_line, tail)) = rest.split_once("\r\n") else {
            break;
        };
        let Ok(size) = usize::from_str_radix(size_line.trim(), 16) else {
            break;
        };
        if size == 0 {
            break;
        }
        if tail.len() < size {
            out.push_str(tail);
            break;
        }
        out.push_str(&tail[..size]);
        rest = tail[size..].trim_start_matches("\r\n");
    }
    out
}

fn urlencode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

struct Target {
    base: String,
    service: String,
    ts_ns: i64,
}

impl Target {
    fn push(&self) -> anyhow::Result<()> {
        let body = serde_json::json!({
            "streams": [{
                "stream": {"service_name": self.service, "pad": "Hello"},
                "values": [[self.ts_ns.to_string(), "audit line"]],
            }]
        })
        .to_string();
        let (status, resp) = http(&self.base, "POST", "/loki/api/v1/push", Some(&body))?;
        if !(200..300).contains(&status) {
            bail!("push to {} failed: {status} {resp}", self.base);
        }
        Ok(())
    }

    fn render(&self, template_body: &str) -> anyhow::Result<String> {
        // Templates in the audit domain contain no backticks.
        let query = format!(
            "{{service_name=\"{}\"}} | line_format `{}`",
            self.service, template_body
        );
        let path = format!(
            "/loki/api/v1/query_range?query={}&start={}&end={}&limit=10&direction=forward",
            urlencode(&query),
            self.ts_ns - 60_000_000_000,
            self.ts_ns + 60_000_000_000,
        );
        let (status, payload) = http(&self.base, "GET", &path, None)?;
        if status != 200 {
            bail!("query failed on {}: {status} {payload}", self.base);
        }
        let parsed: QueryResponse = serde_json::from_str(&payload)
            .with_context(|| format!("parse response from {}", self.base))?;
        let mut lines: Vec<String> = parsed
            .data
            .result
            .iter()
            .flat_map(|s| {
                let err = s.stream.get("__error_details__").cloned();
                s.values.iter().map(move |(_, line)| match &err {
                    Some(e) => format!("{line} [ERR {e}]"),
                    None => line.clone(),
                })
            })
            .collect();
        lines.sort();
        Ok(lines.join("\u{1}"))
    }
}

/// A hex address token: `0x` followed by 4+ hex digits (the reference's
/// heap/global pointers; short literals like `0x2a` in verb output stay
/// under the threshold).
fn address_tokens(s: &str) -> Vec<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i + 2 < bytes.len() {
        if bytes[i] == b'0' && bytes[i + 1] == b'x' {
            let start = i;
            let mut j = i + 2;
            while j < bytes.len() && bytes[j].is_ascii_hexdigit() {
                j += 1;
            }
            if j - (start + 2) >= 4 {
                out.push(s[start..j].to_string());
            }
            i = j;
        } else {
            i += 1;
        }
    }
    out
}

pub async fn run(args: TemplateAuditArgs) -> anyhow::Result<()> {
    // One IDENTICAL fixed absolute timestamp on both sides so ts-driven
    // differences cannot masquerade as pointer-driven ones (plan v7 §C).
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos() as i64;
    let ts_ns = now - 60_000_000_000;
    let run_id = now / 1_000_000_000;
    let a = Target {
        base: args.url_a.clone(),
        service: format!("tplaudit{run_id}"),
        ts_ns,
    };
    let b = Target {
        base: args.url_b.clone(),
        service: format!("tplaudit{run_id}"),
        ts_ns,
    };
    a.push()?;
    b.push()?;
    tokio::time::sleep(std::time::Duration::from_millis(700)).await;

    struct Row {
        name: String,
        out_a: String,
        out_b: String,
        differs: bool,
        tokens: Vec<String>,
    }
    let mut rows = Vec::new();
    for cell in cells() {
        let out_a = a.render(&cell.query_body)?;
        let out_b = b.render(&cell.query_body)?;
        let differs = out_a != out_b;
        let tokens = address_tokens(&out_a);
        rows.push(Row {
            name: cell.name,
            out_a,
            out_b,
            differs,
            tokens,
        });
    }
    // Second pass (plan AC-22): the DECIMAL/OCTAL/BINARY renderings of
    // every hex address found anywhere in the run — the package-global
    // addresses (`%d` of a stock time.Time) are identical across
    // containers AND hex-free, so only this scan sees them.
    let mut numeric_forms: Vec<String> = Vec::new();
    for row in &rows {
        for t in &row.tokens {
            if let Ok(v) = u64::from_str_radix(t.trim_start_matches("0x"), 16) {
                numeric_forms.push(v.to_string());
                numeric_forms.push(format!("{v:o}"));
                numeric_forms.push(format!("{v:b}"));
            }
        }
    }
    numeric_forms.sort_unstable();
    numeric_forms.dedup();
    let mut report = Vec::new();
    let mut n_diff = 0;
    let mut n_addr = 0;
    for row in &mut rows {
        for form in &numeric_forms {
            if row.out_a.contains(form.as_str()) && !row.tokens.iter().any(|t| t == form) {
                row.tokens.push(form.clone());
            }
        }
        if row.differs {
            n_diff += 1;
            println!("DIFF  {}: a={:?} b={:?}", row.name, row.out_a, row.out_b);
        }
        if !row.tokens.is_empty() {
            n_addr += 1;
            println!("ADDR  {}: {:?} in {:?}", row.name, row.tokens, row.out_a);
        }
        report.push(serde_json::json!({
            "cell": row.name,
            "differs_across_containers": row.differs,
            "address_tokens": row.tokens,
            "output_a": row.out_a,
            "output_b": row.out_b,
        }));
    }
    println!(
        "audit: {} cells, {} cross-container diffs, {} address-carrying outputs",
        report.len(),
        n_diff,
        n_addr
    );
    if let Some(path) = args.out {
        let mut text = String::new();
        for row in &report {
            text.push_str(&serde_json::to_string(row)?);
            text.push('\n');
        }
        std::fs::write(&path, text).with_context(|| format!("write {path}"))?;
        println!("wrote {path}");
    }
    Ok(())
}
