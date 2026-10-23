//! The two documents that show every configuration key at its default:
//! docs/configuration.md §9's YAML block and the packaged
//! `packaging/config/config.yaml`. Each is read from its own file, never
//! from a copy, and compared with the model:
//!
//! * its key set equals the model's at every depth, so a missing key, an
//!   extra key and a key in the wrong section each fail; a parse alone
//!   cannot see a missing key, because an omitted key takes its default;
//! * it parses under `deny_unknown_fields` to exactly `Config::default()`,
//!   so every documented value is the built-in one;
//! * every comment that lists a key's spellings lists exactly the ones the
//!   server accepts, and every key that is a choice of spellings has one;
//! * its list examples name every key of a list element.

mod support;

use std::collections::BTreeSet;

use pulsus_config::Config;
use serde_norway::Value;

/// The text of docs/configuration.md after the line `## 9. YAML file`, from
/// the first ```` ```yaml ```` line to the next line starting ```` ``` ````.
fn section_9_block() -> String {
    let doc = support::repo_file("docs/configuration.md");
    let section = doc
        .split_once("\n## 9. YAML file\n")
        .expect("docs/configuration.md has a `## 9. YAML file` line")
        .1;
    first_yaml_block(section, "§9").to_string()
}

/// The first ```` ```yaml ```` block of `text`, up to the next line starting
/// ```` ``` ````.
fn first_yaml_block<'a>(text: &'a str, what: &str) -> &'a str {
    let body = text
        .split_once("```yaml\n")
        .unwrap_or_else(|| panic!("{what} has a ```yaml block"))
        .1;
    body.split_once("\n```")
        .unwrap_or_else(|| panic!("{what}'s ```yaml block is closed"))
        .0
}

/// `packaging/config/config.yaml`, with each line whose trimmed text is `#`
/// immediately followed by `<key>:` (`<key>` non-empty, of `[a-z_]`) losing
/// that `#` and keeping its indentation. Every other line is unchanged, so
/// prose and examples (`# text`, `#   servers:`) stay comments.
fn packaged_config_uncommented() -> String {
    let text = support::repo_file("packaging/config/config.yaml");
    let mut out = String::new();
    for line in text.lines() {
        let trimmed = line.trim_start();
        let indent = &line[..line.len() - trimmed.len()];
        let is_key_line = trimmed.strip_prefix('#').is_some_and(|rest| {
            rest.split_once(':').is_some_and(|(key, _)| {
                !key.is_empty() && key.bytes().all(|b| b.is_ascii_lowercase() || b == b'_')
            })
        });
        if is_key_line {
            out.push_str(indent);
            out.push_str(&trimmed[1..]);
        } else {
            out.push_str(line);
        }
        out.push('\n');
    }
    out
}

fn yaml(text: &str, what: &str) -> Value {
    serde_norway::from_str(text).unwrap_or_else(|e| panic!("{what} must be YAML: {e}"))
}

/// The server's YAML step (`load.rs`'s `read_yaml`): a typed parse under
/// `deny_unknown_fields`.
fn config(text: &str, what: &str) -> Config {
    serde_norway::from_str::<Config>(text)
        .unwrap_or_else(|e| panic!("{what} must parse as the configuration: {e}"))
}

#[test]
fn section_9_names_every_model_key_and_no_other() {
    support::assert_names_every_model_key(
        "docs/configuration.md §9",
        &yaml(&section_9_block(), "docs/configuration.md §9"),
    );
}

#[test]
fn section_9_resolves_to_the_built_in_defaults() {
    assert_eq!(
        config(&section_9_block(), "docs/configuration.md §9"),
        Config::default(),
        "every value in docs/configuration.md §9 must be the built-in default"
    );
}

#[test]
fn the_packaged_config_names_every_model_key_and_no_other() {
    support::assert_names_every_model_key(
        "packaging/config/config.yaml, uncommented",
        &yaml(&packaged_config_uncommented(), "the packaged config"),
    );
}

#[test]
fn the_packaged_config_resolves_to_the_built_in_defaults() {
    assert_eq!(
        config(&packaged_config_uncommented(), "the packaged config"),
        Config::default(),
        "every value in packaging/config/config.yaml, uncommented, must be the built-in default"
    );
}

/// A mapping holding only `value`, at the dotted `path`.
fn one_key(path: &str, value: Value) -> Value {
    path.rsplit('.').fold(value, |inner, step| {
        let mut m = serde_norway::Mapping::new();
        m.insert(Value::String(step.to_string()), inner);
        Value::Mapping(m)
    })
}

/// Whether the server accepts `value` at `path`: it parses, and then it
/// passes `validate`.
fn accepted(path: &str, value: &str) -> bool {
    serde_norway::from_value::<Config>(one_key(path, value.into()))
        .is_ok_and(|cfg| pulsus_config::validate(&cfg).is_ok())
}

/// The spellings the parser names when it refuses an unknown one, from
/// serde's ``unknown variant `x`, expected …`` (``expected `a` ``,
/// ``expected `a` or `b` `` and ``expected one of `a`, `b`, … ``). `None`
/// for a key that is not a choice of spellings.
fn variants(path: &str) -> Option<Vec<String>> {
    let err = serde_norway::from_value::<Config>(one_key(path, "zz_not_a_value".into()))
        .err()?
        .to_string();
    let (_, after) = err.split_once("unknown variant `")?;
    let (_, expected) = after.split_once("`, expected ")?;
    Some(
        expected
            .split('`')
            .skip(1)
            .step_by(2)
            .map(str::to_string)
            .collect(),
    )
}

/// The model key paths whose value is a choice of spellings, found by asking
/// the parser.
fn choice_keys() -> BTreeSet<String> {
    support::model_key_paths()
        .into_iter()
        .filter(|p| variants(p).is_some())
        .collect()
}

/// Each line `key: value  # …` whose comment contains `|`, as its dotted
/// path and the spellings it lists. A top-level key with no value opens a
/// section, and an indented key belongs to it. Each `|`-separated piece
/// gives its first word, trimmed of trailing punctuation. A comment-only
/// line starting `# |` continues the previous list.
fn listed_alternatives(text: &str) -> Vec<(String, Vec<String>)> {
    let mut out: Vec<(String, Vec<String>)> = Vec::new();
    let mut section = String::new();
    let mut continues = false;
    for line in text.lines() {
        let trimmed = line.trim_start();
        if let Some(rest) = trimmed.strip_prefix("# |") {
            if continues {
                let list = &mut out.last_mut().expect("a list to continue").1;
                list.extend(spellings(rest));
            }
            continue;
        }
        continues = false;
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((key, rest)) = trimmed.split_once(':') else {
            continue;
        };
        let (value, comment) = rest.split_once('#').unwrap_or((rest, ""));
        let indented = trimmed.len() != line.len();
        if !indented {
            section = if value.trim().is_empty() {
                key.to_string()
            } else {
                String::new()
            };
        }
        if comment.contains('|') {
            let path = if indented {
                format!("{section}.{key}")
            } else {
                key.to_string()
            };
            out.push((path, spellings(comment)));
            continues = true;
        }
    }
    out
}

fn spellings(comment: &str) -> Vec<String> {
    comment
        .split('|')
        .filter_map(|piece| piece.split_whitespace().next())
        .map(|word| {
            word.trim_end_matches(|c: char| !c.is_alphanumeric())
                .to_string()
        })
        .filter(|word| !word.is_empty())
        .collect()
}

/// A comment of the form `key: value  # a | b | c` is a claim about the
/// parser: it lists exactly the spellings serde names, minus any `validate`
/// then refuses. Every key that is a choice of spellings carries one.
#[test]
fn every_listed_set_of_spellings_is_the_set_the_server_accepts() {
    let want_keys = choice_keys();
    for (what, text) in [
        ("docs/configuration.md §9", section_9_block()),
        (
            "packaging/config/config.yaml, uncommented",
            packaged_config_uncommented(),
        ),
    ] {
        let lists = listed_alternatives(&text);
        let found: BTreeSet<String> = lists.iter().map(|(path, _)| path.clone()).collect();
        assert_eq!(
            found, want_keys,
            "{what}: the keys whose comment lists their spellings"
        );
        for (path, listed) in lists {
            let mut want: Vec<String> = variants(&path)
                .unwrap_or_else(|| panic!("{path} is a choice key"))
                .into_iter()
                .filter(|v| accepted(&path, v))
                .collect();
            want.sort();
            let mut got = listed.clone();
            got.sort();
            assert_eq!(got, want, "{what}: {path} lists {listed:?}");
        }
    }
}

/// Asserts the model's sequence-valued paths are exactly the three list
/// fields, so a new one fails here until it is handled, and returns the
/// key set of a serialised element of each list that holds mappings.
fn element_keys() -> Vec<(&'static str, BTreeSet<String>)> {
    let model = serde_norway::to_value(Config::default()).expect("serialise the defaults");
    let lists: Vec<String> = support::key_paths(&model)
        .into_iter()
        .filter(|path| {
            path.split('.')
                .try_fold(&model, |v, step| v.get(step))
                .is_some_and(Value::is_sequence)
        })
        .collect();
    assert_eq!(
        lists,
        [
            "clickhouse.servers",
            "downsampling.tiers",
            "traceql_indexed_attributes"
        ],
        "the model's list fields"
    );
    let server = pulsus_config::ChServerEntry {
        host: "h".to_string(),
        http_port: Some(1),
        zone: Some("z".to_string()),
    };
    let minute = pulsus_config::HumanDuration(std::time::Duration::from_secs(60));
    let tier = pulsus_config::Tier {
        name: "n".to_string(),
        resolution: minute,
        table: "t".to_string(),
        retention: minute,
        min_step: minute,
    };
    vec![
        (
            "servers",
            support::key_paths(&serde_norway::to_value(server).expect("serialise a server")),
        ),
        (
            "tiers",
            support::key_paths(&serde_norway::to_value(tier).expect("serialise a tier")),
        ),
    ]
}

/// The comment lines after a comment line whose text is exactly `<field>:`,
/// while each one's text starts `- {`, each parsed as a flow mapping.
fn commented_examples(text: &str, field: &str) -> Vec<Value> {
    let opener = format!("{field}:");
    let mut out = Vec::new();
    let mut inside = false;
    for line in text.lines() {
        match line.trim_start().strip_prefix('#').map(str::trim) {
            Some(c) if c == opener => inside = true,
            Some(c) if inside && c.starts_with("- {") => {
                out.push(yaml(&c[2..], &format!("an example {field} entry")));
            }
            _ => inside = false,
        }
    }
    out
}

/// Every list is empty by default, so the key-set tests cannot see inside
/// one. Each document's examples must parse as elements and, together,
/// name every key of an element.
#[test]
fn the_list_examples_name_every_element_key() {
    let documents = [
        ("docs/configuration.md §9", section_9_block()),
        (
            "packaging/config/config.yaml",
            support::repo_file("packaging/config/config.yaml"),
        ),
    ];
    for (field, want) in element_keys() {
        for (what, text) in &documents {
            let examples = commented_examples(text, field);
            assert!(!examples.is_empty(), "{what}: no example {field} entry");
            let mut got = BTreeSet::new();
            for example in &examples {
                got.extend(support::key_paths(example));
                let parsed = match field {
                    "servers" => {
                        serde_norway::from_value::<pulsus_config::ChServerEntry>(example.clone())
                            .map(drop)
                    }
                    "tiers" => {
                        serde_norway::from_value::<pulsus_config::Tier>(example.clone()).map(drop)
                    }
                    other => unreachable!("no element type for {other}"),
                };
                parsed.unwrap_or_else(|e| panic!("{what}: example {field} entry {example:?}: {e}"));
            }
            assert_eq!(got, want, "{what}: the example {field} entries' keys");
        }
    }
}

/// docs/configuration.md §7's first YAML block is a configuration the
/// server starts with, and its tier entries name every tier key.
#[test]
fn section_7_tier_example_is_a_valid_configuration() {
    let doc = support::repo_file("docs/configuration.md");
    let section = doc
        .split_once("\n## 7. ")
        .expect("docs/configuration.md has a §7 heading")
        .1;
    let block = first_yaml_block(section, "§7");
    let cfg = config(block, "§7's example");
    pulsus_config::validate(&cfg).unwrap_or_else(|e| panic!("§7's example must validate: {e}"));
    let tiers = yaml(block, "§7's example")
        .get("downsampling")
        .and_then(|d| d.get("tiers"))
        .and_then(Value::as_sequence)
        .cloned()
        .expect("§7's example has downsampling.tiers");
    let got: BTreeSet<String> = tiers.iter().flat_map(support::key_paths).collect();
    let want = element_keys()
        .into_iter()
        .find(|(field, _)| *field == "tiers")
        .expect("tiers are a list field")
        .1;
    assert_eq!(got, want, "§7's tier entries' keys");
}

/// `docker-compose.yaml` and `otel-config.yaml` each say they mirror
/// docs/configuration.md §10's YAML verbatim. Compared by value, so the
/// files' own header comments may differ.
#[test]
fn the_quickstart_files_are_section_10s_yaml() {
    let doc = support::repo_file("docs/configuration.md");
    let section = doc
        .split_once("\n## 10. ")
        .expect("docs/configuration.md has a §10 heading")
        .1;
    let blocks: Vec<&str> = section
        .split("```yaml\n")
        .skip(1)
        .map(|b| b.split_once("\n```").expect("a closed ```yaml block").0)
        .collect();
    assert_eq!(blocks.len(), 2, "§10 has two ```yaml blocks");
    for (block, file) in blocks
        .iter()
        .zip(["docker-compose.yaml", "otel-config.yaml"])
    {
        assert_eq!(
            yaml(block, "§10"),
            yaml(&support::repo_file(file), file),
            "{file} must be §10's YAML"
        );
    }
}

/// Issue #506 part 3: docs/configuration.md §7 and docs/architecture.md
/// §3.1.1 say the `downsampling` settings are parsed and validated and that
/// nothing else reads them. This holds the second half: no source file
/// outside this crate names them. When downsampling is built, this fails,
/// and the two "Not built yet" paragraphs go with it.
#[test]
fn nothing_outside_the_config_crate_reads_the_downsampling_settings() {
    let names = regex::Regex::new(
        r"\.downsampling\b|DownsamplingConfig|TierPolicy|tier_policy|raw_retention",
    )
    .unwrap();
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let listing = std::process::Command::new("git")
        .arg("-C")
        .arg(&root)
        .args(["ls-files", "-z", "--", "crates"])
        .output()
        .expect("git ls-files must run");
    let mut sources = 0usize;
    let mut readers = Vec::new();
    for file in String::from_utf8(listing.stdout).unwrap().split('\0') {
        if !file.contains("/src/")
            || !file.ends_with(".rs")
            || file.starts_with("crates/pulsus-config/")
        {
            continue;
        }
        sources += 1;
        let text = std::fs::read_to_string(root.join(file)).unwrap_or_default();
        for (i, line) in text.lines().enumerate() {
            if names.is_match(line) {
                readers.push(format!("{file}:{}: {}", i + 1, line.trim()));
            }
        }
    }
    assert!(sources > 100, "read only {sources} source files");
    assert!(
        readers.is_empty(),
        "the downsampling settings are read: {readers:#?}"
    );
}
