//! Issue #624, part 3d (side change V): every pullable ClickHouse server or
//! keeper image reference in the tracked tree — `clickhouse/clickhouse-server`
//! or `-keeper` followed by `:` or `@` — is the supported floor,
//! 26.8.21.10, by tag or by its registry digest. A record of an earlier
//! measurement names its server in prose ("server 24.8"), never as a
//! reference. The one exception is the CI leg that proves an older server is
//! refused, which must be there exactly once.

// The image names are written without a tag here, so that this file, which
// the test reads like any other, holds no pullable reference of its own.
const SERVER: &str = "clickhouse/clickhouse-server";
const KEEPER: &str = "clickhouse/clickhouse-keeper";
const FLOOR_TAG: &str = "26.8.21.10";
const SERVER_DIGEST: &str =
    "sha256:d3fd302416d9523d5d64239d5713d1c16c20ecea2045ac70d7e941b376cb266e";
const KEEPER_DIGEST: &str =
    "sha256:112ce72cf0ad4c48b5884159d40c4821914c06e3df3e73da61a5b69398960c63";
/// The CI leg that proves an older server is refused: its file and tag.
const REFUSAL_LEG: (&str, &str) = (".github/workflows/ci.yml", "24.8");

fn floor(reference: &str) -> bool {
    reference == format!("{SERVER}:{FLOOR_TAG}")
        || reference == format!("{KEEPER}:{FLOOR_TAG}")
        || reference == format!("{SERVER}@{SERVER_DIGEST}")
        || reference == format!("{KEEPER}@{KEEPER_DIGEST}")
        || reference == format!("{SERVER}:{FLOOR_TAG}@{SERVER_DIGEST}")
}

#[test]
fn every_server_image_pin_is_the_floor() {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
    let out = std::process::Command::new("git")
        .args(["ls-files"])
        .current_dir(root)
        .output()
        .expect("git ls-files");
    let files = String::from_utf8(out.stdout).expect("utf-8");
    let mut refusal = 0usize;
    let mut pins = 0usize;
    let mut wrong: Vec<String> = Vec::new();
    for file in files.lines() {
        let Ok(text) = std::fs::read_to_string(format!("{root}/{file}")) else {
            continue;
        };
        for (n, line) in text.lines().enumerate() {
            let mut rest = line;
            while let Some(at) = rest.find("clickhouse/clickhouse-") {
                let tail = &rest[at..];
                let len = tail
                    .find(|c: char| !(c.is_ascii_alphanumeric() || "/._:@-".contains(c)))
                    .unwrap_or(tail.len());
                let reference = tail[..len].trim_end_matches(['.', ':']);
                rest = &tail[len.max(1)..];
                let named = reference
                    .strip_prefix(SERVER)
                    .or_else(|| reference.strip_prefix(KEEPER));
                if !matches!(named, Some(v) if v.starts_with(':') || v.starts_with('@')) {
                    continue;
                }
                pins += 1;
                if floor(reference) {
                    continue;
                }
                if file == REFUSAL_LEG.0 && reference == format!("{SERVER}:{}", REFUSAL_LEG.1) {
                    refusal += 1;
                } else {
                    wrong.push(format!("{file}:{}: {reference}", n + 1));
                }
            }
        }
    }
    if refusal != 1 {
        wrong.push(format!("the refusal leg found {refusal} times, want 1"));
    }
    assert!(pins >= 90, "{pins} versioned references found");
    assert!(
        wrong.is_empty(),
        "{} findings:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
}
