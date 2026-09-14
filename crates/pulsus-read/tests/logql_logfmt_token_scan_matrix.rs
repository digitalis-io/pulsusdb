//! Issue #507: `| logfmt` over 400 generated lines holding malformed tokens,
//! under `| logfmt` and `| logfmt --strict` — 800 line–mode pairs — against
//! the labels the pinned reference returned for each.
//!
//! **The lines.** `tests/fixtures/logfmt_token_scan/lines.tsv`: seed 5077,
//! 1 to 8 tokens each drawn from 30 fragments that reach every decoder error
//! class — a quote or `=` in a key, a quote or `=` after an unquoted value,
//! an unterminated quote, an escaped quote, a control byte, a multibyte
//! rune, JSON punctuation. Hex-encoded, so control bytes survive the file.
//!
//! **The reference's labels.** `tests/fixtures/logfmt_token_scan/reference_labels.tsv`:
//! grafana/loki:3.7.4 (`b318f282`, digest
//! `sha256:87f0a067673756a3cede1bcbf0c74875f7df9b09fddb53e399d0c576f756cfcc`)
//! on `ci/logql/config.yaml`, each line pushed as its own stream and read
//! back with the query, 2026-09-13. Stream labels and the reference's own
//! `detected_level` are removed; the error pair and every parsed label stay,
//! the details text included.
//!
//! **What the hermetic half asserts.** Our pipeline's labels equal the
//! reference's on every pair except exactly [`DIFFER`], and on those they
//! differ. Every id in [`DIFFER`] is the rule recorded as
//! `logfmt-quoted-value-ends-its-token` in
//! `docs/benchmarks/logs-differential-ledger.md`: the reference reads a
//! quoted pair the moment its closing quote arrives and starts the next key
//! at the very next byte (`pkg/logql/log/logfmt/decode.go:151-186 @ v3.7.4`),
//! and skips a malformed token to the next separator with no notion of
//! quotes (`:140-149`); we treat a quoted value its closing quote does not
//! end as a malformed token and never read a label from inside a value.
//!
//! **What the live half asserts.** Gated on `PULSUSDB_LOGQL_DIFF_URL`: it
//! pushes the 400 lines to that reference in one stream, reads them back
//! under both modes, and asserts the answers equal the committed labels —
//! so the committed file cannot drift from the build it names.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use pulsus_read::logql::pipeline::CompiledPipeline;

/// The line–mode pairs whose labels differ from the reference's: `l` is
/// `| logfmt`, `s` is `| logfmt --strict`.
const DIFFER: &[&str] = &[
    "L016l", "L019l", "L019s", "L038l", "L038s", "L042l", "L042s", "L059l", "L059s", "L062l",
    "L062s", "L068l", "L068s", "L071l", "L072l", "L072s", "L079l", "L079s", "L081l", "L081s",
    "L089l", "L089s", "L099l", "L099s", "L116l", "L116s", "L118l", "L118s", "L121l", "L121s",
    "L130l", "L130s", "L132l", "L132s", "L140l", "L155l", "L155s", "L173l", "L173s", "L179l",
    "L179s", "L185l", "L185s", "L202l", "L202s", "L204l", "L204s", "L207l", "L207s", "L209l",
    "L230l", "L230s", "L234l", "L234s", "L237l", "L237s", "L240l", "L240s", "L241l", "L241s",
    "L242l", "L242s", "L248l", "L248s", "L276l", "L288l", "L288s", "L293l", "L293s", "L306l",
    "L314l", "L314s", "L324l", "L324s", "L327l", "L327s", "L350l", "L350s", "L354l", "L377l",
    "L377s", "L379l", "L379s", "L381l", "L381s", "L387l", "L387s", "L394l", "L394s", "L398l",
    "L398s",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Mode {
    Lenient,
    Strict,
}

impl Mode {
    fn suffix(self) -> char {
        match self {
            Mode::Lenient => 'l',
            Mode::Strict => 's',
        }
    }
    fn stage(self) -> &'static str {
        match self {
            Mode::Lenient => "| logfmt",
            Mode::Strict => "| logfmt --strict",
        }
    }
    fn parse(text: &str) -> Mode {
        match text {
            "lenient" => Mode::Lenient,
            "strict" => Mode::Strict,
            other => panic!("unknown mode {other:?}"),
        }
    }
}

type Labels = BTreeMap<String, String>;

fn fixture(name: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/logfmt_token_scan")
        .join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"))
}

fn unhex(text: &str) -> String {
    let bytes: Vec<u8> = (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).expect("hex"))
        .collect();
    String::from_utf8(bytes).expect("every generated line is UTF-8")
}

/// `(id, line)` in file order.
fn lines() -> Vec<(String, String)> {
    fixture("lines.tsv")
        .lines()
        .filter(|l| !l.starts_with('#'))
        .map(|l| {
            let (id, hex) = l.split_once('\t').expect("id<TAB>hex");
            (id.to_string(), unhex(hex))
        })
        .collect()
}

fn reference_labels() -> HashMap<(String, Mode), Labels> {
    fixture("reference_labels.tsv")
        .lines()
        .filter(|l| !l.starts_with('#'))
        .map(|l| {
            let mut parts = l.splitn(3, '\t');
            let id = parts.next().expect("id").to_string();
            let mode = Mode::parse(parts.next().expect("mode"));
            let labels: Labels =
                serde_json::from_str(parts.next().expect("labels")).expect("labels json");
            ((id, mode), labels)
        })
        .collect()
}

/// Our pipeline's labels for one line, without the stream's own label.
fn our_labels(pipeline: &CompiledPipeline, line: &str) -> Labels {
    let base = [("service_name".to_string(), "lf".to_string())];
    let out = pipeline
        .run(line, &base, 0)
        .expect("no budget breach")
        .expect("logfmt keeps every line");
    out.labels
        .iter()
        .filter(|(k, _)| k.as_ref() != "service_name")
        .map(|(k, v): &(Cow<'_, str>, Cow<'_, str>)| (k.to_string(), v.to_string()))
        .collect()
}

fn compiled(mode: Mode) -> CompiledPipeline {
    let query = format!(r#"{{service_name="lf"}} {}"#, mode.stage());
    let pulsus_logql::Expr::Log(log) = pulsus_logql::parse(&query).expect("parse") else {
        panic!("a log query");
    };
    CompiledPipeline::compile(&log.pipeline).expect("compile")
}

#[test]
fn the_fixtures_hold_every_line_under_both_modes() {
    let lines = lines();
    assert_eq!(lines.len(), 400);
    for (i, (id, _)) in lines.iter().enumerate() {
        assert_eq!(id, &format!("L{i:03}"));
    }
    let reference = reference_labels();
    assert_eq!(reference.len(), 800);
    for (id, _) in &lines {
        for mode in [Mode::Lenient, Mode::Strict] {
            assert!(reference.contains_key(&(id.clone(), mode)), "{id} {mode:?}");
        }
    }
}

#[test]
fn our_labels_differ_from_the_reference_exactly_on_the_listed_pairs() {
    let reference = reference_labels();
    let mut differ: BTreeSet<String> = BTreeSet::new();
    let mut first: BTreeMap<String, String> = BTreeMap::new();
    for mode in [Mode::Lenient, Mode::Strict] {
        let pipeline = compiled(mode);
        for (id, line) in lines() {
            let ours = our_labels(&pipeline, &line);
            let theirs = &reference[&(id.clone(), mode)];
            if &ours != theirs {
                let key = format!("{id}{}", mode.suffix());
                first.insert(
                    key.clone(),
                    format!("{line:?}: ours {ours:?}, the reference {theirs:?}"),
                );
                differ.insert(key);
            }
        }
    }
    let want: BTreeSet<String> = DIFFER.iter().map(|s| s.to_string()).collect();
    let unexpected: Vec<&String> = differ.difference(&want).collect();
    let missing: Vec<&String> = want.difference(&differ).collect();
    assert!(
        unexpected.is_empty() && missing.is_empty(),
        "differ from the reference but are not listed: {:?}\nlisted but equal to the reference: {missing:?}",
        unexpected
            .iter()
            .map(|k| format!("{k} {}", first[*k]))
            .collect::<Vec<_>>()
    );
}

// ---------------------------------------------------------------------
// The live half (gated on PULSUSDB_LOGQL_DIFF_URL).
// ---------------------------------------------------------------------

fn curl(args: &[&str]) -> (String, String) {
    let out = Command::new("curl")
        .args(["-s", "--max-time", "30", "-w", "\n%{http_code}"])
        .args(args)
        .output()
        .expect("curl must be on PATH");
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    let (body, status) = text.rsplit_once('\n').unwrap_or((&text, ""));
    (body.to_string(), status.to_string())
}

/// The stream's own labels, which the committed file does not carry.
const STREAM_LABELS: &[&str] = &["app", "service_name", "detected_level"];

#[test]
fn live_the_committed_reference_labels_match_the_reference() {
    let Some(base) = pulsus_testkit::live_endpoint("PULSUSDB_LOGQL_DIFF_URL") else {
        eprintln!("PULSUSDB_LOGQL_DIFF_URL unset; skipping the live logfmt token-scan matrix");
        return;
    };
    let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("clock");
    let app = format!("lf507-{}", now.as_secs());
    let lines = lines();
    // One stream, one entry per line, 1 ms apart and ending a minute ago.
    let first_ns = now.as_nanos() - 60_000_000_000 - lines.len() as u128 * 1_000_000;
    let id_at: HashMap<String, String> = lines
        .iter()
        .enumerate()
        .map(|(i, (id, _))| ((first_ns + i as u128 * 1_000_000).to_string(), id.clone()))
        .collect();
    let values: Vec<serde_json::Value> = lines
        .iter()
        .enumerate()
        .map(|(i, (_, line))| {
            serde_json::json!([(first_ns + i as u128 * 1_000_000).to_string(), line])
        })
        .collect();
    let push = serde_json::json!({"streams": [{"stream": {"app": app}, "values": values}]});
    let deadline = std::time::Instant::now() + Duration::from_secs(180);
    loop {
        let (body, status) = curl(&[
            "-H",
            "Content-Type: application/json",
            "-X",
            "POST",
            "--data-binary",
            &push.to_string(),
            &format!("{base}/loki/api/v1/push"),
        ]);
        if status == "204" {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the reference refused the push: {status} {body}"
        );
        std::thread::sleep(Duration::from_secs(2));
    }
    let start = (first_ns / 1_000_000_000 - 60).to_string();
    let end = (now.as_secs() + 60).to_string();
    let reference = reference_labels();
    let mut wrong = Vec::new();
    for mode in [Mode::Lenient, Mode::Strict] {
        let query = format!(r#"{{app="{app}"}} {}"#, mode.stage());
        let got: HashMap<String, Labels> = loop {
            let (body, status) = curl(&[
                "-G",
                "--data-urlencode",
                &format!("query={query}"),
                "--data-urlencode",
                &format!("start={start}"),
                "--data-urlencode",
                &format!("end={end}"),
                "--data-urlencode",
                "limit=1000",
                "--data-urlencode",
                "direction=forward",
                &format!("{base}/loki/api/v1/query_range"),
            ]);
            let mut got = HashMap::new();
            if status == "200" {
                let parsed: serde_json::Value = serde_json::from_str(&body).expect("json");
                for stream in parsed["data"]["result"].as_array().into_iter().flatten() {
                    let labels: Labels = stream["stream"]
                        .as_object()
                        .expect("stream labels")
                        .iter()
                        .filter(|(k, _)| !STREAM_LABELS.contains(&k.as_str()))
                        .map(|(k, v)| (k.clone(), v.as_str().expect("string").to_string()))
                        .collect();
                    for value in stream["values"].as_array().into_iter().flatten() {
                        let ts = value[0].as_str().expect("timestamp").to_string();
                        if let Some(id) = id_at.get(&ts) {
                            got.insert(id.clone(), labels.clone());
                        }
                    }
                }
            }
            if got.len() == lines.len() {
                break got;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{query}: {} of {} entries visible ({status})",
                got.len(),
                lines.len()
            );
            std::thread::sleep(Duration::from_secs(1));
        };
        for (id, _) in &lines {
            let committed = &reference[&(id.clone(), mode)];
            if &got[id] != committed {
                wrong.push(format!(
                    "{id} {mode:?}: live {:?}, committed {committed:?}",
                    got[id]
                ));
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "the live reference answers differently from the committed labels:\n{}",
        wrong.join("\n")
    );
}
