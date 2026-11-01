//! **1-U3 (issue #494 part 1): on a cluster each landing table has one
//! routing wrapper, keyed on the push's receive stamp.**
//!
//! Every row of a push carries the one `received_ms` the writer took for it,
//! so `cityHash64(received_ms)` sends the whole block, and every resend of
//! it, to one shard. Single-node renders no wrapper. Read from what
//! `schema/schema.sh --print` writes, which is what a deployment applies.

use std::process::Command;

const LANDING: [&str; 3] = ["log_landing", "metric_landing", "trace_landing"];

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
fn each_landing_table_has_one_wrapper_on_the_receive_stamp_on_a_cluster() {
    let (code, text, err) =
        print_schema(&[("PULSUS_RETENTION_DAYS", "7"), ("PULSUS_CLUSTER", "c")]);
    assert_eq!(code, Some(0), "the script refused: {err}");
    for table in LANDING {
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
                && engine.ends_with(&format!("'{table}', cityHash64(received_ms));")),
            "{table}_dist routes {table} on the receive stamp: {engine}"
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
