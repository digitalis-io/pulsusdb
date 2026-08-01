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
        // One budget per probed line — the same per-ROW lifetime the
        // pipeline gives a real row (issue #260).
        let budget = template::RenderBudget::default();
        let result: Result<String, _> = match &compiled {
            Template::Simple(name) => Ok(labels
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.to_string())
                .unwrap_or_default()),
            Template::Parts(parts) => {
                let mut rendered = String::new();
                for part in parts {
                    match part {
                        template::Part::Lit(s) => rendered.push_str(s),
                        template::Part::Field(name) => {
                            if let Some((_, v)) = labels.iter().find(|(k, _)| k == name) {
                                rendered.push_str(v);
                            }
                        }
                    }
                }
                Ok(rendered)
            }
            Template::Full(prog) => {
                template::render_full(prog, &labels, None, None, body, ts_ns, &env, &budget)
                    .map(|r| r.as_str().to_string())
            }
        };
        match result {
            Ok(rendered) => {
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
