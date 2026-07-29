//! DEV-ONLY differential probe for the issue #230 template engine:
//! reads JSONL cases (`{kind, template, labels, line, ts_ns}`) on stdin
//! and emits the engine's `{compile_err | out, err, details}` per line.
//! Pair it with an in-checkout oracle built against the pinned
//! reference (a ~60-line `main.go` that feeds the same JSONL through
//! `log.NewFormatter`/`NewLabelsFormatter` and prints the same shape),
//! run both under the same `TZ`, and diff — the workflow that
//! byte-verified this engine over ~725 cases before the container
//! capture. Not part of any test suite; the committed contract is the
//! container-captured corpus (`tests/logqltest/corpus/t*.test`).

use std::borrow::Cow;
use std::io::{BufRead, Write};

use pulsus_read::logql::template::{self, Template, TemplateEnv, TemplateKind};

fn main() {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut out = std::io::BufWriter::new(stdout.lock());
    // Mirrors the engine's process-environment resolution so the
    // oracle and the probe can be driven under the same TZ.
    let env = TemplateEnv::process();
    for line in stdin.lock().lines() {
        let Ok(line) = line else { break };
        if line.trim().is_empty() {
            continue;
        }
        let case: serde_json::Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => {
                let _ = writeln!(out, "{{\"compile_err\":\"bad case json\"}}");
                continue;
            }
        };
        let kind = match case.get("kind").and_then(|v| v.as_str()) {
            Some("label") => TemplateKind::Label,
            _ => TemplateKind::Line,
        };
        let tmpl_text = case
            .get("template")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let body = case.get("line").and_then(|v| v.as_str()).unwrap_or("");
        let ts_ns = case.get("ts_ns").and_then(|v| v.as_i64()).unwrap_or(0);
        let mut labels: Vec<(Cow<'_, str>, Cow<'_, str>)> = Vec::new();
        if let Some(map) = case.get("labels").and_then(|v| v.as_object()) {
            for (k, v) in map {
                if let Some(v) = v.as_str() {
                    labels.push((Cow::Owned(k.clone()), Cow::Owned(v.to_string())));
                }
            }
        }
        labels.sort();

        let compiled = match template::compile(tmpl_text, kind) {
            Ok(t) => t,
            Err(e) => {
                let _ = writeln!(
                    out,
                    "{}",
                    serde_json::json!({ "compile_err": e.to_string() })
                );
                continue;
            }
        };
        let mut buf = Vec::new();
        let result = match &compiled {
            Template::Simple(name) => {
                let v = labels
                    .iter()
                    .find(|(k, _)| k == name)
                    .map(|(_, v)| v.to_string())
                    .unwrap_or_default();
                buf.extend_from_slice(v.as_bytes());
                Ok(())
            }
            Template::Parts(parts) => {
                for part in parts {
                    match part {
                        template::Part::Lit(s) => buf.extend_from_slice(s.as_bytes()),
                        template::Part::Field(name) => {
                            if let Some((_, v)) = labels.iter().find(|(k, _)| k == name) {
                                buf.extend_from_slice(v.as_bytes());
                            }
                        }
                    }
                }
                Ok(())
            }
            Template::Full(prog) => {
                template::render_full(prog, &labels, None, None, body, ts_ns, &env, &mut buf)
            }
        };
        match result {
            Ok(()) => {
                let rendered = String::from_utf8_lossy(&buf).into_owned();
                let _ = writeln!(out, "{}", serde_json::json!({ "out": rendered }));
            }
            Err(e) => {
                // line_format keeps the line; label_format leaves the
                // label unset — the probe reports the raw error text.
                let keep = match kind {
                    TemplateKind::Line => body.to_string(),
                    TemplateKind::Label => String::new(),
                };
                let _ = writeln!(
                    out,
                    "{}",
                    serde_json::json!({
                        "out": keep,
                        "err": "TemplateFormatErr",
                        "details": e.msg,
                    })
                );
            }
        }
    }
}
