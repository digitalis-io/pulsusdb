//! **2-U3 (issue #494 part 1b): on a cluster each landing table has one
//! routing wrapper.** `log_landing` and `trace_landing` are keyed on the
//! push's receive stamp, so `cityHash64(received_ms)` sends a whole log or
//! trace block, and every resend the writer makes of it, to one shard.
//! `metric_landing` is keyed on `cityHash64(fingerprint)`, the key of every
//! metrics table's wrapper, so all of a series' rows sit on one shard.
//! Single-node renders no wrapper. Read from what `schema/schema.sh --print`
//! writes, which is what a deployment applies.

use std::process::Command;

const LANDING: [&str; 3] = ["log_landing", "metric_landing", "trace_landing"];

/// Each landing table and the expression its wrapper is keyed on.
const KEYED: [(&str, &str); 3] = [
    ("log_landing", "cityHash64(received_ms)"),
    ("metric_landing", "cityHash64(fingerprint)"),
    ("trace_landing", "cityHash64(received_ms)"),
];

/// `schema.sh --print` with an empty environment but for `PATH` and `vars`:
/// the exit status and the statements it printed.
fn print_schema(vars: &[(&str, &str)]) -> (Option<i32>, String, String) {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut cmd = Command::new("sh");
    cmd.arg(root.join("schema/schema.sh"))
        .arg("--print")
        .current_dir(&root)
        .env_clear()
        .env("PATH", std::env::var("PATH").expect("PATH is set"));
    for (var, value) in vars {
        cmd.env(var, value);
    }
    let out = cmd.output().expect("run schema.sh");
    (
        out.status.code(),
        String::from_utf8(out.stdout).expect("the schema is UTF-8"),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// The lines of `text` that open a `CREATE TABLE` naming `<table>_dist`, each
/// with the line after it.
fn wrapper_heads<'a>(text: &'a str, table: &str) -> Vec<(&'a str, &'a str)> {
    let lines: Vec<&str> = text.lines().collect();
    let name = format!(".{table}_dist ");
    lines
        .iter()
        .enumerate()
        .filter(|(_, l)| l.starts_with("CREATE TABLE") && l.contains(&name))
        .map(|(i, l)| (*l, lines.get(i + 1).copied().unwrap_or("")))
        .collect()
}

#[test]
fn each_landing_table_has_one_wrapper_on_its_key_on_a_cluster() {
    let (code, text, err) =
        print_schema(&[("PULSUS_RETENTION_DAYS", "7"), ("PULSUS_CLUSTER", "c")]);
    assert_eq!(code, Some(0), "the script refused: {err}");
    for (table, key) in KEYED {
        let heads = wrapper_heads(&text, table);
        assert_eq!(
            heads.len(),
            1,
            "exactly one `CREATE TABLE … {table}_dist`: {heads:?}"
        );
        let (create, engine) = heads[0];
        assert!(
            create.ends_with(&format!(" AS pulsus.{table}")),
            "{table}_dist takes {table}'s columns: {create}"
        );
        assert!(
            engine.starts_with("ENGINE = Distributed('c', ")
                && engine.ends_with(&format!("'{table}', {key});")),
            "{table}_dist routes {table} on {key}: {engine}"
        );
    }
}

#[test]
fn single_node_renders_no_landing_wrapper() {
    let (code, text, err) = print_schema(&[("PULSUS_RETENTION_DAYS", "7")]);
    assert_eq!(code, Some(0), "the script refused: {err}");
    for table in LANDING {
        assert!(
            !text.contains(&format!("{table}_dist")),
            "single-node names no {table}_dist"
        );
    }
}
