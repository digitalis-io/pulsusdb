//! The `logqltest` DSL parser + hermetic replayer (issue #220, Batch 0).
//!
//! A `.test` file is a sequence of blocks, promqltest-style:
//!
//! ```text
//! # a comment
//! load
//!   {env="prod", service_name="svc-json"} service=svc-json
//!   0s  {"status":500,"method":"GET"}
//!   5s  {"status":200,"method":"POST"}
//!
//! eval instant at 60s {service_name="svc-json"} | json | status = "500"
//!   {env="prod", method="GET", service_name="svc-json", status="500"} 0s {"status":500,"method":"GET"}
//!
//! eval instant at 60s count_over_time({service_name="svc-json"} | json | status = "500" [30m])
//!   {service_name="svc-json"} 1
//!
//! clear
//! ```
//!
//! Directives (column 0, blank-line separated): `clear`, `load`,
//! `eval instant at <T> <query>`, `eval_ordered instant at <T> <query>`,
//! `eval_fail instant at <T> <query>`, and — issue #227 — `eval range from
//! <T0> to <T1> step <S> <query>` (the Loki sliding-window range evaluator;
//! a matrix result compared point-by-point at EXACT-f64). Instant and range
//! goldens are bit-exact against `grafana/loki:3.7.4` (range values captured
//! from the container, or hand-derived for integer-exact reducers verified
//! against `pkg/logql/range_vector.go` — see `PROVENANCE.md`).
//!
//! Evaluation drives the SAME pure functions the engine executes — for a
//! log query `CompiledPipeline::run` per loaded line; for a metric query
//! `plan()` → `run_client_agg_rows`/`apply_vector_aggs`/`combine_binary`
//! at `QuerySpec::Instant` — and compares with EXACT-f64 equality
//! (`f64::to_bits`, no tolerance — the #218 discipline). No ClickHouse.
//!
//! Dataset == query scope: within one `load`/`clear` section, an eval sees
//! every loaded stream (the stream selector is not re-applied — the real
//! engine pushes it to SQL). Author each section so its loaded streams are
//! exactly the scope its evals select; use `clear` to switch scope.

use std::collections::HashMap;

use pulsus_logql::{Expr, parse};
use pulsus_read::logql::rows::{MetricScanRow, StreamMetaRow};
use pulsus_read::logql::{
    ClientWindow, CompiledPipeline, Direction, MatrixSeries, MetricNode, MetricPlan, Plan, PlanCtx,
    QueryParams, QueryResult, QuerySpec, apply_vector_aggs, combine_binary, materialize_vector_lit,
    plan, run_client_agg_rows, run_variants_rows,
};

/// A sorted label set.
type Labels = Vec<(String, String)>;
/// One resolved streams entry: `(sorted labels, timestamp_ns, line)`.
type StreamEntry = (Labels, i64, String);
/// One resolved instant-vector sample: `(sorted labels, value)`.
type VectorEntry = (Labels, f64);

/// The plan context the corpus evaluates under — the same table/budget
/// shape the hermetic metric-agg goldens use (`logql_metric_agg_golden.rs`).
fn ctx() -> PlanCtx<'static> {
    PlanCtx {
        db: "pulsus",
        streams_idx: "log_streams_idx",
        streams: "log_streams",
        samples: "log_samples",
        rollup_table: "log_metrics_5s",
        rollup_res_ns: 5_000_000_000,
        scan_budget_bytes: 50 * 1024 * 1024 * 1024,
        max_streams: 100_000,
        pipeline_scan_factor: 10,
    }
}

// ---------------------------------------------------------------------
// Duration + label-set literals.
// ---------------------------------------------------------------------

/// Parses a Loki/Prometheus-style duration (`0s`, `250ms`, `1m`, `1h30m`,
/// `1ns`) into nanoseconds. Compound units accumulate left to right.
pub fn parse_duration_ns(s: &str) -> Result<i64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty duration".to_string());
    }
    let (neg, body) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s),
    };
    let mut total: i64 = 0;
    let mut chars = body.char_indices().peekable();
    let mut any = false;
    while let Some(&(start, c)) = chars.peek() {
        if !c.is_ascii_digit() {
            return Err(format!("duration {s:?}: expected a number at {c:?}"));
        }
        // Scan the numeric run.
        let mut end = start;
        while let Some(&(i, d)) = chars.peek() {
            if d.is_ascii_digit() {
                end = i + d.len_utf8();
                chars.next();
            } else {
                break;
            }
        }
        let num: i64 = body[start..end]
            .parse()
            .map_err(|e| format!("duration {s:?}: bad number: {e}"))?;
        // Scan the unit run (alphabetic + the micro sign).
        let unit_start = end;
        let mut unit_end = end;
        while let Some(&(i, u)) = chars.peek() {
            if u.is_ascii_alphabetic() || u == 'µ' || u == 'μ' {
                unit_end = i + u.len_utf8();
                chars.next();
            } else {
                break;
            }
        }
        let unit = &body[unit_start..unit_end];
        let mult: i64 = match unit {
            "ns" => 1,
            "us" | "µs" | "μs" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60_000_000_000,
            "h" => 3_600_000_000_000,
            "d" => 86_400_000_000_000,
            "w" => 604_800_000_000_000,
            "" => return Err(format!("duration {s:?}: missing unit after {num}")),
            other => return Err(format!("duration {s:?}: unknown unit {other:?}")),
        };
        total = total
            .checked_add(num.checked_mul(mult).ok_or("duration overflow")?)
            .ok_or("duration overflow")?;
        any = true;
    }
    if !any {
        return Err(format!("duration {s:?}: no components"));
    }
    Ok(if neg { -total } else { total })
}

/// Consumes a `{ k="v", k2="v2" }` label set from the START of `s`,
/// returning the (unsorted) pairs and the byte length consumed (just past
/// the closing `}`). Values are double-quoted with `\"`, `\\`, `\n`, `\t`,
/// `\r` escapes; keys are bare identifiers.
pub fn parse_labelset(s: &str) -> Result<(Vec<(String, String)>, usize), String> {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if i >= bytes.len() || bytes[i] != b'{' {
        return Err(format!("expected a label set `{{...}}` at {s:?}"));
    }
    i += 1;
    let mut pairs = Vec::new();
    loop {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() {
            return Err(format!("unterminated label set in {s:?}"));
        }
        if bytes[i] == b'}' {
            i += 1;
            break;
        }
        if bytes[i] == b',' {
            i += 1;
            continue;
        }
        // key
        let key_start = i;
        while i < bytes.len()
            && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' || bytes[i] == b'.')
        {
            i += 1;
        }
        if i == key_start {
            return Err(format!("expected a label name at byte {i} in {s:?}"));
        }
        let key = s[key_start..i].to_string();
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] != b'=' {
            return Err(format!("expected `=` after {key:?} in {s:?}"));
        }
        i += 1;
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] != b'"' {
            return Err(format!("expected a quoted value for {key:?} in {s:?}"));
        }
        i += 1;
        let mut value = String::new();
        loop {
            if i >= bytes.len() {
                return Err(format!("unterminated string value for {key:?} in {s:?}"));
            }
            let c = bytes[i];
            if c == b'\\' {
                i += 1;
                if i >= bytes.len() {
                    return Err(format!("dangling escape for {key:?} in {s:?}"));
                }
                match bytes[i] {
                    b'"' => value.push('"'),
                    b'\\' => value.push('\\'),
                    b'n' => value.push('\n'),
                    b't' => value.push('\t'),
                    b'r' => value.push('\r'),
                    other => {
                        value.push('\\');
                        value.push(other as char);
                    }
                }
                i += 1;
            } else if c == b'"' {
                i += 1;
                break;
            } else {
                // Copy one UTF-8 char.
                let ch_len = utf8_len(c);
                value.push_str(&s[i..i + ch_len]);
                i += ch_len;
            }
        }
        pairs.push((key, value));
    }
    Ok((pairs, i))
}

fn utf8_len(first: u8) -> usize {
    if first < 0x80 {
        1
    } else if first >> 5 == 0b110 {
        2
    } else if first >> 4 == 0b1110 {
        3
    } else {
        4
    }
}

/// Renders label pairs as PulsusDB's canonical flat JSON (sorted keys),
/// the shape `parse_flat_labels` reads back.
fn labels_to_json(pairs: &[(String, String)]) -> String {
    let mut sorted = pairs.to_vec();
    sorted.sort();
    let mut out = String::from("{");
    for (idx, (k, v)) in sorted.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        json_string(k, &mut out);
        out.push(':');
        json_string(v, &mut out);
    }
    out.push('}');
    out
}

fn json_string(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            other => out.push(other),
        }
    }
    out.push('"');
}

// ---------------------------------------------------------------------
// Parsed commands.
// ---------------------------------------------------------------------

/// One declared stream in a `load` block.
#[derive(Debug, Clone)]
pub struct StreamSpec {
    pub labels: Vec<(String, String)>,
    pub service: String,
    /// `(timestamp_ns, body)`.
    pub samples: Vec<(i64, String)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvalMode {
    /// Compare as a set (streams / instant vector).
    Value,
    /// Compare as an ordered list (`sort`/`sort_desc`).
    Ordered,
    /// The query must error; the (required) assert line matches the
    /// produced error text — `msg:` as a substring, `msg_exact:`
    /// byte-exactly (issue #240).
    Fail,
}

/// A range eval's grid: `range from <T0> to <T1> step <S>` (issue #227).
#[derive(Debug, Clone, Copy)]
pub struct RangeEval {
    pub start_ns: i64,
    pub end_ns: i64,
    pub step_ns: u64,
}

#[derive(Debug, Clone)]
pub struct EvalCmd {
    pub line: usize,
    pub at_ns: i64,
    /// `Some` for a `range from <T0> to <T1> step <S>` eval (issue #227); the
    /// matrix result's `(ts, value)` points are compared exactly. `None` =
    /// the instant eval (uses `at_ns`).
    pub range: Option<RangeEval>,
    pub mode: EvalMode,
    pub query: String,
    /// Raw expected result lines (interpreted at compare time by result
    /// kind); empty = an empty result.
    pub expected: Vec<String>,
    /// `eval_fail` only: the required error assertion (issue #240 —
    /// exactly one per `eval_fail`, enforced at parse time).
    pub fail_msg: Option<FailAssert>,
}

/// `eval_fail`'s single assert line (issue #240): `msg:` gates a
/// substring; `msg_exact:` gates the WHOLE produced error, byte-exactly.
/// Values are trimmed — a named limitation; none of the pinned bodies has
/// significant leading/trailing whitespace.
#[derive(Debug, Clone)]
pub enum FailAssert {
    Contains(String),
    Exact(String),
}

#[derive(Debug, Clone)]
pub enum Command {
    Clear,
    Load(Vec<StreamSpec>),
    Eval(EvalCmd),
}

/// Counts of each executed directive — the corpus test asserts every kind
/// is exercised (mirrors promqltest's grammar-coverage gate).
#[derive(Debug, Clone, Copy, Default)]
pub struct DirectiveCounts {
    pub clear: usize,
    pub load: usize,
    pub eval_value: usize,
    pub eval_ordered: usize,
    pub eval_fail: usize,
    pub streams_cases: usize,
    pub vector_cases: usize,
    pub scalar_cases: usize,
    /// Range (`eval range`) matrix results (issue #227).
    pub matrix_cases: usize,
}

/// One executed case's outcome.
#[derive(Debug, Clone)]
pub struct CaseReport {
    pub line: usize,
    pub query: String,
    pub mode: EvalMode,
    pub passed: bool,
    pub detail: String,
}

#[derive(Debug, Clone)]
pub struct FileRun {
    pub file: String,
    pub cases: Vec<CaseReport>,
    pub counts: DirectiveCounts,
}

// ---------------------------------------------------------------------
// Parsing.
// ---------------------------------------------------------------------

fn is_comment_or_blank(line: &str) -> bool {
    let t = line.trim();
    t.is_empty() || t.starts_with('#')
}

fn is_indented(line: &str) -> bool {
    line.starts_with(' ') || line.starts_with('\t')
}

/// Parses a `.test` file into its command stream. `Err` = a grammar error
/// (hard, loud, file-fatal), not a per-case failure.
pub fn parse_file(file: &str, text: &str) -> Result<Vec<Command>, String> {
    let lines: Vec<&str> = text.lines().collect();
    let mut commands = Vec::new();
    let mut idx = 0;
    while idx < lines.len() {
        let raw = lines[idx];
        if is_comment_or_blank(raw) {
            idx += 1;
            continue;
        }
        if is_indented(raw) {
            return Err(format!(
                "{file}:{}: indented line without an owning directive: {raw:?}",
                idx + 1
            ));
        }
        let directive = raw.trim();
        if directive == "clear" {
            commands.push(Command::Clear);
            idx += 1;
        } else if directive == "load" {
            let (streams, next) = parse_load_block(file, &lines, idx + 1)?;
            commands.push(Command::Load(streams));
            idx = next;
        } else if let Some(rest) = strip_eval_prefix(directive) {
            let (cmd, next) = parse_eval(file, rest, &lines, idx)?;
            commands.push(Command::Eval(cmd));
            idx = next;
        } else {
            return Err(format!(
                "{file}:{}: unknown directive: {directive:?}",
                idx + 1
            ));
        }
    }
    Ok(commands)
}

/// Splits a `load` block's indented body into streams. A header line is
/// `{labels} [service=x]`; a sample line is `<offset> <body>`. Both attach
/// to the most recent header.
fn parse_load_block(
    file: &str,
    lines: &[&str],
    start: usize,
) -> Result<(Vec<StreamSpec>, usize), String> {
    let mut streams: Vec<StreamSpec> = Vec::new();
    let mut idx = start;
    while idx < lines.len() {
        let raw = lines[idx];
        if raw.trim().is_empty() {
            idx += 1;
            break;
        }
        if is_comment_or_blank(raw) {
            idx += 1;
            continue;
        }
        if !is_indented(raw) {
            break;
        }
        let content = raw.trim();
        if content.starts_with('{') {
            let (labels, consumed) = parse_labelset(content).map_err(|e| fmt_err(file, idx, e))?;
            let tail = content[consumed..].trim();
            let service = if let Some(svc) = tail.strip_prefix("service=") {
                svc.trim().to_string()
            } else if !tail.is_empty() {
                return Err(fmt_err(
                    file,
                    idx,
                    format!("unexpected text after stream labels: {tail:?}"),
                ));
            } else {
                labels
                    .iter()
                    .find(|(k, _)| k == "service_name")
                    .map(|(_, v)| v.clone())
                    .unwrap_or_default()
            };
            streams.push(StreamSpec {
                labels,
                service,
                samples: Vec::new(),
            });
        } else {
            let stream = streams
                .last_mut()
                .ok_or_else(|| fmt_err(file, idx, "sample line before any stream header".into()))?;
            let (offset, body) = content
                .split_once(char::is_whitespace)
                .ok_or_else(|| fmt_err(file, idx, "sample line needs `<offset> <body>`".into()))?;
            let ts = parse_duration_ns(offset).map_err(|e| fmt_err(file, idx, e))?;
            stream.samples.push((ts, body.trim_start().to_string()));
        }
        idx += 1;
    }
    Ok((streams, idx))
}

/// Strips the `eval`/`eval_ordered`/`eval_fail` verb; returns
/// `(mode, "instant at <T> <query>")`.
fn strip_eval_prefix(directive: &str) -> Option<(EvalMode, &str)> {
    if let Some(rest) = directive.strip_prefix("eval_ordered ") {
        Some((EvalMode::Ordered, rest))
    } else if let Some(rest) = directive.strip_prefix("eval_fail ") {
        Some((EvalMode::Fail, rest))
    } else if let Some(rest) = directive.strip_prefix("eval ") {
        Some((EvalMode::Value, rest))
    } else {
        None
    }
}

fn parse_eval(
    file: &str,
    (mode, rest): (EvalMode, &str),
    lines: &[&str],
    directive_idx: usize,
) -> Result<(EvalCmd, usize), String> {
    let (at_ns, range, query) = if let Some(rest) = rest.strip_prefix("instant at ") {
        let (at_tok, query) = rest
            .split_once(char::is_whitespace)
            .ok_or_else(|| fmt_err(file, directive_idx, "eval needs `<T> <query>`".into()))?;
        let at_ns = parse_duration_ns(at_tok).map_err(|e| fmt_err(file, directive_idx, e))?;
        (at_ns, None, query.trim().to_string())
    } else if let Some(rest) = rest.strip_prefix("range from ") {
        // `<T0> to <T1> step <S> <query>` (issue #227). The query may contain
        // spaces, so split off exactly the 5 fixed grid tokens first.
        let parts: Vec<&str> = rest.splitn(6, ' ').collect();
        if parts.len() != 6 || parts[1] != "to" || parts[3] != "step" {
            return Err(fmt_err(
                file,
                directive_idx,
                "eval range needs `from <T0> to <T1> step <S> <query>`".into(),
            ));
        }
        let start_ns = parse_duration_ns(parts[0]).map_err(|e| fmt_err(file, directive_idx, e))?;
        let end_ns = parse_duration_ns(parts[2]).map_err(|e| fmt_err(file, directive_idx, e))?;
        let step_i = parse_duration_ns(parts[4]).map_err(|e| fmt_err(file, directive_idx, e))?;
        if step_i <= 0 {
            return Err(fmt_err(
                file,
                directive_idx,
                "eval range step must be > 0".into(),
            ));
        }
        (
            0,
            Some(RangeEval {
                start_ns,
                end_ns,
                step_ns: step_i as u64,
            }),
            parts[5].trim().to_string(),
        )
    } else {
        return Err(fmt_err(
            file,
            directive_idx,
            "only `instant at <T>` and `range from <T0> to <T1> step <S>` evals are supported"
                .into(),
        ));
    };

    let mut expected = Vec::new();
    let mut fail_msg = None;
    let mut idx = directive_idx + 1;
    while idx < lines.len() {
        let raw = lines[idx];
        if raw.trim().is_empty() {
            idx += 1;
            break;
        }
        if !is_indented(raw) {
            break;
        }
        let content = raw.trim();
        if content.starts_with('#') {
            idx += 1;
            continue;
        }
        if mode == EvalMode::Fail {
            // Exactly ONE assert line per eval_fail; `msg_exact:` first
            // (note `"msg_exact:".strip_prefix("msg:")` is None — the
            // fourth byte is `_` — so the order is for clarity only).
            let assert = if let Some(msg) = content.strip_prefix("msg_exact:") {
                FailAssert::Exact(msg.trim().to_string())
            } else if let Some(msg) = content.strip_prefix("msg:") {
                FailAssert::Contains(msg.trim().to_string())
            } else {
                return Err(fmt_err(
                    file,
                    idx,
                    format!(
                        "eval_fail expects a `msg: <substring>` or `msg_exact: <text>` line, \
                         got {content:?}"
                    ),
                ));
            };
            let value = match &assert {
                FailAssert::Contains(v) | FailAssert::Exact(v) => v,
            };
            if value.is_empty() {
                return Err(fmt_err(
                    file,
                    idx,
                    "eval_fail assert line has an empty value".into(),
                ));
            }
            if fail_msg.is_some() {
                return Err(fmt_err(
                    file,
                    idx,
                    "eval_fail carries more than one assert line (exactly one \
                     `msg:`/`msg_exact:` is required)"
                        .into(),
                ));
            }
            fail_msg = Some(assert);
        } else {
            expected.push(content.to_string());
        }
        idx += 1;
    }

    if mode == EvalMode::Fail && fail_msg.is_none() {
        return Err(fmt_err(
            file,
            directive_idx,
            "eval_fail requires exactly one `msg: <substring>` or `msg_exact: <text>` line".into(),
        ));
    }

    Ok((
        EvalCmd {
            line: directive_idx + 1,
            at_ns,
            range,
            mode,
            query,
            expected,
            fail_msg,
        },
        idx,
    ))
}

fn fmt_err(file: &str, idx: usize, msg: String) -> String {
    format!("{file}:{}: {msg}", idx + 1)
}

// ---------------------------------------------------------------------
// The hermetic store.
// ---------------------------------------------------------------------

#[derive(Debug, Clone)]
struct StoredStream {
    fingerprint: u64,
    base: Vec<(String, String)>,
    samples: Vec<(i64, String)>,
}

#[derive(Debug, Default)]
struct Store {
    meta: HashMap<u64, StreamMetaRow>,
    rows: Vec<MetricScanRow>,
    streams: Vec<StoredStream>,
    next_fp: u64,
}

impl Store {
    fn clear(&mut self) {
        self.meta.clear();
        self.rows.clear();
        self.streams.clear();
        // fingerprints stay monotone across a clear (identity is irrelevant
        // to the pure functions; distinctness is all that matters).
    }

    fn load(&mut self, streams: &[StreamSpec]) {
        for spec in streams {
            self.next_fp += 1;
            let fp = self.next_fp;
            let mut base = spec.labels.clone();
            base.sort();
            self.meta.insert(
                fp,
                StreamMetaRow {
                    fingerprint: fp,
                    service: spec.service.clone(),
                    labels: labels_to_json(&spec.labels),
                },
            );
            for (ts, body) in &spec.samples {
                self.rows.push(MetricScanRow {
                    fingerprint: fp,
                    timestamp_ns: *ts,
                    body: body.clone(),
                });
            }
            self.streams.push(StoredStream {
                fingerprint: fp,
                base,
                samples: spec.samples.clone(),
            });
        }
    }
}

// ---------------------------------------------------------------------
// Evaluation.
// ---------------------------------------------------------------------

/// A resolved query result — either log streams or a metric value.
enum Outcome {
    /// `(sorted labels, timestamp_ns, line)` per surviving entry.
    Streams(Vec<StreamEntry>),
    Metric(QueryResult),
}

fn evaluate(store: &Store, query: &str, spec: QuerySpec) -> Result<Outcome, String> {
    let expr = parse(query).map_err(|e| e.to_string())?;
    match &expr {
        Expr::Log(log) => {
            if matches!(spec, QuerySpec::Range { .. }) {
                return Err("a `range` eval requires a metric query".to_string());
            }
            let compiled = CompiledPipeline::compile(&log.pipeline).map_err(|e| e.to_string())?;
            let mut out = Vec::new();
            for stream in &store.streams {
                for (ts, body) in &stream.samples {
                    if let Some(entry) = compiled.run(body, &stream.base) {
                        let mut labels: Vec<(String, String)> = entry
                            .labels
                            .iter()
                            .map(|(k, v)| (k.to_string(), v.to_string()))
                            .collect();
                        labels.sort();
                        out.push((labels, *ts, entry.line.into_owned()));
                    }
                }
            }
            Ok(Outcome::Streams(out))
        }
        Expr::Metric(_) => {
            let params = QueryParams {
                spec,
                limit: 100,
                direction: Direction::Backward,
            };
            let result = match plan(&expr, &params, &ctx()).map_err(|e| e.to_string())? {
                Plan::Metric(mp) => eval_leaf(&mp, store)?,
                Plan::MetricBinary(node) => eval_node(&node, store)?,
                Plan::Streams(_) => {
                    return Err("a metric expression planned to a streams query".to_string());
                }
            };
            Ok(Outcome::Metric(result))
        }
    }
}

/// Evaluates one client-aggregated metric leaf over the store — the
/// engine's exact post-fetch sequence (compile → aggregate → vector aggs).
fn eval_leaf(mp: &MetricPlan, store: &Store) -> Result<QueryResult, String> {
    let client = mp.client.as_ref().ok_or_else(|| {
        "logqltest supports only client-aggregated (raw-scan) metric plans (Batch 0)".to_string()
    })?;
    let compiled = CompiledPipeline::compile(&client.pipeline).map_err(|e| e.to_string())?;
    let result = run_client_agg_rows(
        &store.rows,
        &compiled,
        &store.meta,
        client,
        match mp.step_ns {
            Some(step_ns) => ClientWindow::Range {
                grid_start_ns: mp.grid_start_ns,
                end_ns: mp.end_ns,
                step_ns,
                range_ns: mp.range_ns,
            },
            None => ClientWindow::Instant {
                start_ns: mp.grid_start_ns,
                end_ns: mp.end_ns,
            },
        },
        mp.rate_window_ns,
    )
    .map_err(|e| e.to_string())?;
    Ok(apply_vector_aggs(result, &mp.vector_aggs))
}

fn eval_node(node: &MetricNode, store: &Store) -> Result<QueryResult, String> {
    match node {
        MetricNode::Scalar(v) => Ok(QueryResult::Scalar(*v)),
        MetricNode::VectorLit { value, window } => {
            materialize_vector_lit(*value, window).map_err(|e| e.to_string())
        }
        MetricNode::Leaf(mp) => eval_leaf(mp, store),
        MetricNode::Binary {
            op,
            return_bool,
            matching,
            lhs,
            rhs,
        } => {
            let l = eval_node(lhs, store)?;
            let r = eval_node(rhs, store)?;
            combine_binary(*op, *return_bool, matching.as_ref(), l, r).map_err(|e| e.to_string())
        }
        MetricNode::VectorAgg { aggs, inner } => {
            Ok(apply_vector_aggs(eval_node(inner, store)?, aggs))
        }
        // `variants(...) of (...)` (issue #221): the pure twin of the
        // live engine's fan-out — the SAME `VariantArena` +
        // `VariantsAggState`, so corpus cases exercise the identical
        // charging path. The common pipeline comes from the scan plan
        // (already truncated at any dead common-range `unwrap`).
        MetricNode::Variants { scan, variants, .. } => {
            let common = scan
                .client
                .as_ref()
                .ok_or_else(|| "variants scan plan must be client-aggregated".to_string())?;
            run_variants_rows(&store.rows, &store.meta, &common.pipeline, variants)
                .map_err(|e| e.to_string())
        }
    }
}

// ---------------------------------------------------------------------
// Comparison.
// ---------------------------------------------------------------------

fn judge(cmd: &EvalCmd, outcome: Result<Outcome, String>) -> (bool, String) {
    match cmd.mode {
        EvalMode::Fail => match outcome {
            Ok(_) => (
                false,
                "eval_fail expected the query to error but it succeeded".to_string(),
            ),
            Err(text) => match &cmd.fail_msg {
                Some(FailAssert::Contains(m)) if !text.contains(m.as_str()) => (
                    false,
                    format!("error {text:?} does not contain the required substring {m:?}"),
                ),
                Some(FailAssert::Exact(m)) if text != *m => (
                    false,
                    format!("error {text:?} != the required exact text {m:?}"),
                ),
                _ => (true, String::new()),
            },
        },
        EvalMode::Value | EvalMode::Ordered => match outcome {
            Err(text) => (false, format!("query errored: {text}")),
            Ok(Outcome::Streams(actual)) => compare_streams(&cmd.expected, actual),
            Ok(Outcome::Metric(result)) => compare_metric(cmd, result),
        },
    }
}

fn compare_streams(expected: &[String], actual: Vec<StreamEntry>) -> (bool, String) {
    let mut want = Vec::new();
    for line in expected {
        match parse_expected_stream(line) {
            Ok(e) => want.push(e),
            Err(e) => return (false, format!("bad expected stream line: {e}")),
        }
    }
    let mut got = actual;
    want.sort();
    got.sort();
    if want == got {
        (true, String::new())
    } else {
        (
            false,
            format!(
                "streams mismatch:\n  want ({}): {}\n  got  ({}): {}",
                want.len(),
                fmt_streams(&want),
                got.len(),
                fmt_streams(&got),
            ),
        )
    }
}

fn parse_expected_stream(line: &str) -> Result<StreamEntry, String> {
    let (mut labels, consumed) = parse_labelset(line)?;
    labels.sort();
    let rest = line[consumed..].trim_start();
    let (ts_tok, body) = rest
        .split_once(char::is_whitespace)
        .ok_or_else(|| format!("expected `{{labels}} <ts> <line>`: {line:?}"))?;
    let ts = parse_duration_ns(ts_tok)?;
    Ok((labels, ts, body.trim_start().to_string()))
}

fn fmt_streams(items: &[StreamEntry]) -> String {
    items
        .iter()
        .map(|(l, ts, line)| format!("{} @{ts} {line:?}", fmt_labels(l)))
        .collect::<Vec<_>>()
        .join("; ")
}

fn compare_metric(cmd: &EvalCmd, result: QueryResult) -> (bool, String) {
    match result {
        QueryResult::Vector(items) => {
            let mut actual: Vec<VectorEntry> = items
                .into_iter()
                .map(|s| {
                    let mut labels = s.labels;
                    labels.sort();
                    (labels, s.value)
                })
                .collect();
            let mut want: Vec<VectorEntry> = Vec::new();
            for line in &cmd.expected {
                match parse_expected_vector(line) {
                    Ok(e) => want.push(e),
                    Err(e) => return (false, format!("bad expected vector line: {e}")),
                }
            }
            if cmd.mode == EvalMode::Ordered {
                compare_vector_ordered(&want, &actual)
            } else {
                want.sort_by(|a, b| a.0.cmp(&b.0));
                actual.sort_by(|a, b| a.0.cmp(&b.0));
                compare_vector_ordered(&want, &actual)
            }
        }
        QueryResult::Scalar(v) => {
            if cmd.expected.len() != 1 {
                return (
                    false,
                    format!(
                        "scalar result expects exactly one bare value line, got {:?}",
                        cmd.expected
                    ),
                );
            }
            match cmd.expected[0].trim().parse::<f64>() {
                Ok(want) if want.to_bits() == v.to_bits() => (true, String::new()),
                Ok(want) => (false, format!("scalar mismatch: got {v}, want {want}")),
                Err(e) => (
                    false,
                    format!("bad expected scalar {:?}: {e}", cmd.expected[0]),
                ),
            }
        }
        QueryResult::Matrix(series) => compare_matrix(&cmd.expected, series),
        other => (
            false,
            format!("unexpected result kind for an instant eval: {other:?}"),
        ),
    }
}

/// Compares a range (`eval range`) matrix result (issue #227): each expected
/// line is `{labels} <offset1> <value1> <offset2> <value2> ...` — the series'
/// sorted `(timestamp_ns, value)` points, compared with EXACT-f64 equality.
/// An empty window emits no point, so the offsets are explicit (gaps show as
/// missing offsets, never as zeros).
fn compare_matrix(expected: &[String], series: Vec<MatrixSeries>) -> (bool, String) {
    let mut want: Vec<(Labels, Vec<(i64, f64)>)> = Vec::new();
    for line in expected {
        match parse_expected_matrix(line) {
            Ok(e) => want.push(e),
            Err(e) => return (false, format!("bad expected matrix line: {e}")),
        }
    }
    let mut got: Vec<(Labels, Vec<(i64, f64)>)> = series
        .into_iter()
        .map(|s| {
            let mut labels = s.labels;
            labels.sort();
            let mut points = s.points;
            points.sort_by_key(|(t, _)| *t);
            (labels, points)
        })
        .collect();
    want.sort_by(|a, b| a.0.cmp(&b.0));
    got.sort_by(|a, b| a.0.cmp(&b.0));
    if want.len() != got.len() {
        return (
            false,
            format!(
                "series count mismatch: got {}, want {}",
                got.len(),
                want.len()
            ),
        );
    }
    for (w, g) in want.iter().zip(&got) {
        if w.0 != g.0 {
            return (
                false,
                format!(
                    "series label mismatch: got {}, want {}",
                    fmt_labels(&g.0),
                    fmt_labels(&w.0)
                ),
            );
        }
        if w.1.len() != g.1.len() {
            return (
                false,
                format!(
                    "point count mismatch for {}: got {:?}, want {:?}",
                    fmt_labels(&w.0),
                    g.1,
                    w.1
                ),
            );
        }
        for ((wt, wv), (gt, gv)) in w.1.iter().zip(&g.1) {
            if wt != gt || wv.to_bits() != gv.to_bits() {
                return (
                    false,
                    format!(
                        "point mismatch for {}: got ({gt}, {gv}), want ({wt}, {wv})",
                        fmt_labels(&w.0),
                    ),
                );
            }
        }
    }
    (true, String::new())
}

fn parse_expected_matrix(line: &str) -> Result<(Labels, Vec<(i64, f64)>), String> {
    let (mut labels, consumed) = parse_labelset(line)?;
    labels.sort();
    let rest = line[consumed..].trim();
    let toks: Vec<&str> = rest.split_whitespace().collect();
    if !toks.len().is_multiple_of(2) {
        return Err(format!(
            "matrix line needs `<offset> <value>` pairs after the labels: {line:?}"
        ));
    }
    let mut points = Vec::new();
    for pair in toks.chunks(2) {
        let ts = parse_duration_ns(pair[0])?;
        let v = pair[1]
            .parse::<f64>()
            .map_err(|e| format!("bad value {:?}: {e}", pair[1]))?;
        points.push((ts, v));
    }
    Ok((labels, points))
}

fn compare_vector_ordered(want: &[VectorEntry], actual: &[VectorEntry]) -> (bool, String) {
    if want.len() != actual.len() {
        return (
            false,
            format!(
                "series count mismatch: got {} [{}], want {} [{}]",
                actual.len(),
                fmt_vector(actual),
                want.len(),
                fmt_vector(want),
            ),
        );
    }
    for (i, (w, g)) in want.iter().zip(actual).enumerate() {
        if w.0 != g.0 {
            return (
                false,
                format!(
                    "label mismatch at position {i}: got {}, want {}",
                    fmt_labels(&g.0),
                    fmt_labels(&w.0)
                ),
            );
        }
        if w.1.to_bits() != g.1.to_bits() {
            return (
                false,
                format!(
                    "value mismatch at position {i} for {}: got {}, want {} (exact f64 bits differ)",
                    fmt_labels(&w.0),
                    g.1,
                    w.1
                ),
            );
        }
    }
    (true, String::new())
}

fn parse_expected_vector(line: &str) -> Result<VectorEntry, String> {
    let (mut labels, consumed) = parse_labelset(line)?;
    labels.sort();
    let value_str = line[consumed..].trim();
    let value = value_str
        .parse::<f64>()
        .map_err(|e| format!("bad value {value_str:?}: {e}"))?;
    Ok((labels, value))
}

fn fmt_labels(labels: &[(String, String)]) -> String {
    let mut out = String::from("{");
    for (i, (k, v)) in labels.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(k);
        out.push('=');
        out.push_str(&format!("{v:?}"));
    }
    out.push('}');
    out
}

fn fmt_vector(items: &[VectorEntry]) -> String {
    items
        .iter()
        .map(|(l, v)| format!("{} {v}", fmt_labels(l)))
        .collect::<Vec<_>>()
        .join("; ")
}

// ---------------------------------------------------------------------
// Driver.
// ---------------------------------------------------------------------

/// Parses and replays one `.test` file. `Err` = a grammar error (file
/// fatal); individual case failures are reported per case.
pub fn run_file(file: &str, text: &str) -> Result<FileRun, String> {
    let commands = parse_file(file, text)?;
    let mut store = Store::default();
    let mut cases = Vec::new();
    let mut counts = DirectiveCounts::default();

    for command in &commands {
        match command {
            Command::Clear => {
                counts.clear += 1;
                store.clear();
            }
            Command::Load(streams) => {
                counts.load += 1;
                store.load(streams);
            }
            Command::Eval(cmd) => {
                match cmd.mode {
                    EvalMode::Value => counts.eval_value += 1,
                    EvalMode::Ordered => counts.eval_ordered += 1,
                    EvalMode::Fail => counts.eval_fail += 1,
                }
                let spec = match cmd.range {
                    Some(r) => QuerySpec::Range {
                        start_ns: r.start_ns,
                        end_ns: r.end_ns,
                        step_ns: r.step_ns,
                    },
                    None => QuerySpec::Instant { at_ns: cmd.at_ns },
                };
                let outcome = evaluate(&store, &cmd.query, spec);
                match &outcome {
                    Ok(Outcome::Streams(_)) => counts.streams_cases += 1,
                    Ok(Outcome::Metric(QueryResult::Scalar(_))) => counts.scalar_cases += 1,
                    Ok(Outcome::Metric(QueryResult::Matrix(_))) => counts.matrix_cases += 1,
                    Ok(Outcome::Metric(_)) => counts.vector_cases += 1,
                    Err(_) => {}
                }
                let (passed, detail) = judge(cmd, outcome);
                cases.push(CaseReport {
                    line: cmd.line,
                    query: cmd.query.clone(),
                    mode: cmd.mode,
                    passed,
                    detail,
                });
            }
        }
    }

    Ok(FileRun {
        file: file.to_string(),
        cases,
        counts,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_parser_handles_units_and_compounds() {
        assert_eq!(parse_duration_ns("0s").unwrap(), 0);
        assert_eq!(parse_duration_ns("5s").unwrap(), 5_000_000_000);
        assert_eq!(parse_duration_ns("250ms").unwrap(), 250_000_000);
        assert_eq!(parse_duration_ns("1m").unwrap(), 60_000_000_000);
        assert_eq!(parse_duration_ns("1h30m").unwrap(), 5_400_000_000_000);
        assert_eq!(parse_duration_ns("1ns").unwrap(), 1);
        assert!(parse_duration_ns("5").is_err(), "a unit is required");
        assert!(parse_duration_ns("").is_err());
    }

    #[test]
    fn labelset_parser_reads_pairs_and_escapes() {
        let (pairs, consumed) = parse_labelset(r#"{env="prod", msg="a\"b\\c"} tail"#).unwrap();
        assert_eq!(
            pairs,
            vec![
                ("env".to_string(), "prod".to_string()),
                ("msg".to_string(), "a\"b\\c".to_string()),
            ]
        );
        assert_eq!(
            r#"{env="prod", msg="a\"b\\c"} tail"#[consumed..].trim(),
            "tail"
        );
    }

    #[test]
    fn empty_labelset_is_valid() {
        let (pairs, _) = parse_labelset("{} 1").unwrap();
        assert!(pairs.is_empty());
    }
}
