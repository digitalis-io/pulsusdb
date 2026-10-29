//! The setup form's generator (issue #746 part C): tests C1-C9, C11, C12,
//! C12b and C15-C17. The fixture `example` is the issue's example
//! (`pulsus_setup::example_pairs`).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use pulsus_setup::{Inputs, bundle, example_pairs, validate, write_bundle};

type Pairs = Vec<(String, String)>;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// `pairs` with each `(name, value)` set, replacing the field's value.
fn with(mut pairs: Pairs, overrides: &[(&str, &str)]) -> Pairs {
    for (name, value) in overrides {
        pairs.retain(|(n, _)| n != name);
        pairs.push((name.to_string(), value.to_string()));
    }
    pairs
}

fn example() -> Pairs {
    example_pairs()
}

/// C5: `example` with every width blank.
fn zero_levels() -> Pairs {
    with(
        example(),
        &[
            ("l1_width", ""),
            ("l2_width", ""),
            ("l3_width", ""),
            ("l4_width", ""),
        ],
    )
}

/// C6: `example` with level 1 at 30 local and 335 object-storage days.
fn mixed_levels() -> Pairs {
    with(example(), &[("l1_local", "30"), ("l1_remote", "335")])
}

/// C7: `example` with no remote storage.
fn no_remote() -> Pairs {
    with(example(), &[("remote_url", ""), ("raw_remote_days", "")])
}

/// C8: `example` with no TLS.
fn no_tls() -> Pairs {
    with(
        example(),
        &[("tls_cert", ""), ("tls_key", ""), ("tls_ca", "")],
    )
}

fn validated(pairs: &Pairs) -> pulsus_setup::Validated {
    validate(&Inputs::from_pairs(pairs.clone()))
        .unwrap_or_else(|e| panic!("the inputs are refused: {e:?}"))
}

/// The bundle of `pairs`, by path.
fn files(pairs: &Pairs) -> BTreeMap<String, String> {
    let files = bundle(&validated(pairs));
    let mut out = BTreeMap::new();
    for f in files {
        assert!(
            out.insert(f.path.clone(), f.content).is_none(),
            "{} is in the bundle twice",
            f.path
        );
    }
    out
}

fn file<'a>(files: &'a BTreeMap<String, String>, path: &str) -> &'a str {
    files
        .get(path)
        .unwrap_or_else(|| panic!("no {path} in {:?}", files.keys().collect::<Vec<_>>()))
}

/// `None` when `pairs` are accepted, else every `(field, message)`.
fn refusals(pairs: &Pairs) -> Option<Vec<(String, String)>> {
    match validate(&Inputs::from_pairs(pairs.clone())) {
        Ok(_) => None,
        Err(errors) => Some(errors.into_iter().map(|e| (e.field, e.message)).collect()),
    }
}

/// The messages refused against `field`; empty when `field` is accepted.
fn field_messages(pairs: &Pairs, field: &str) -> Vec<String> {
    refusals(pairs)
        .unwrap_or_default()
        .into_iter()
        .filter(|(f, _)| f == field)
        .map(|(_, m)| m)
        .collect()
}

fn lines(text: &str) -> Vec<&str> {
    text.lines().collect()
}

/// `env`'s `NAME=value` lines as pairs.
fn env_pairs(env: &str) -> Vec<(String, String)> {
    env.lines()
        .map(|l| {
            let (k, v) = l
                .split_once('=')
                .unwrap_or_else(|| panic!("not NAME=value: {l:?}"));
            (k.to_string(), v.to_string())
        })
        .collect()
}

/// `sh schema/schema.sh --mode cluster --print` from `dir`, with exactly
/// the variables of `schema_env` and `PATH`.
fn script_print(dir: &Path, schema_env: &str) -> (Option<i32>, String, String) {
    let out = Command::new("sh")
        .arg("schema/schema.sh")
        .args(["--mode", "cluster", "--print"])
        .current_dir(dir)
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .envs(env_pairs(schema_env))
        .output()
        .expect("run sh");
    (
        out.status.code(),
        String::from_utf8(out.stdout).expect("utf-8"),
        String::from_utf8(out.stderr).expect("utf-8"),
    )
}

/// **C1: `schema.env` is the §5.3 variables, sorted.**
#[test]
fn c1_schema_env_of_the_example() {
    let files = files(&example());
    assert_eq!(
        lines(file(&files, "schema.env")),
        [
            "CLICKHOUSE_DB=pulsus",
            "PULSUS_CLUSTER=pulsus_cluster",
            "PULSUS_COLD_VOLUME=cold",
            "PULSUS_LOGS_HOT_DAYS=7",
            "PULSUS_METRICS_HOT_DAYS=7",
            "PULSUS_RETENTION_DAYS=97",
            "PULSUS_ROLLUP_LEVELS=60,720,7200,86400",
            "PULSUS_ROLLUP_RANGES=3600,86400,604800,7776000",
            "PULSUS_ROLLUP_RETENTION_DAYS=365,365,365,365",
            "PULSUS_STORAGE_POLICY=pulsus_tiered",
            "PULSUS_TRACES_HOT_DAYS=7",
        ]
    );
}

const NODE_4: &str = "<clickhouse>
    <macros>
        <shard>2</shard>
        <replica>ch4</replica>
        <cluster>pulsus_cluster</cluster>
    </macros>
    <interserver_http_host>ch4</interserver_http_host>
</clickhouse>
";

const CLUSTER_TLS: &str = "<clickhouse>
    <listen_host>::</listen_host>
    <listen_host>0.0.0.0</listen_host>
    <listen_try>1</listen_try>
    <remote_servers>
        <pulsus_cluster>
            <secret from_env=\"CLICKHOUSE_INTERSERVER_SECRET\"/>
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>ch1</host>
                    <port>9440</port>
                    <secure>1</secure>
                </replica>
                <replica>
                    <host>ch2</host>
                    <port>9440</port>
                    <secure>1</secure>
                </replica>
                <replica>
                    <host>ch3</host>
                    <port>9440</port>
                    <secure>1</secure>
                </replica>
            </shard>
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>ch4</host>
                    <port>9440</port>
                    <secure>1</secure>
                </replica>
                <replica>
                    <host>ch5</host>
                    <port>9440</port>
                    <secure>1</secure>
                </replica>
                <replica>
                    <host>ch6</host>
                    <port>9440</port>
                    <secure>1</secure>
                </replica>
            </shard>
        </pulsus_cluster>
    </remote_servers>
    <zookeeper>
        <node>
            <host>kp1</host>
            <port>9181</port>
            <secure>1</secure>
        </node>
        <node>
            <host>kp2</host>
            <port>9181</port>
            <secure>1</secure>
        </node>
        <node>
            <host>kp3</host>
            <port>9181</port>
            <secure>1</secure>
        </node>
    </zookeeper>
    <interserver_http_credentials>
        <user>interserver</user>
        <password from_env=\"CLICKHOUSE_INTERSERVER_SECRET\"/>
    </interserver_http_credentials>
</clickhouse>
";

/// **C2: host 4 is replica `ch4` of shard 2, and the cluster file lists
/// both shards over TLS.**
#[test]
fn c2_node_and_cluster_files_of_host_4() {
    let files = files(&example());
    assert_eq!(file(&files, "nodes/4/config.d/pulsus-node.xml"), NODE_4);
    let cluster = file(&files, "nodes/4/config.d/pulsus-cluster.xml");
    assert_eq!(cluster, CLUSTER_TLS);
    assert!(!cluster.contains("keeper_server"));
    // Identical on every host.
    for i in 1..=6 {
        assert_eq!(
            file(&files, &format!("nodes/{i}/config.d/pulsus-cluster.xml")),
            CLUSTER_TLS,
            "host {i}"
        );
    }
}

/// **C3: `schema-rendered.sql` is what the script prints from
/// `schema.env`.**
#[test]
fn c3_rendered_schema_is_what_the_script_prints() {
    for (name, pairs) in [
        ("example", example()),
        ("zero levels", zero_levels()),
        ("a level that moves", mixed_levels()),
    ] {
        let files = files(&pairs);
        let (code, out, err) = script_print(&repo_root(), file(&files, "schema.env"));
        assert_eq!(code, Some(0), "{name}: {err}");
        assert_eq!(out, file(&files, "schema-rendered.sql"), "{name}");
    }
}

/// One C4 case: its name, the overrides of `example`, and every
/// `(field, message)` refused.
type C4Case = (
    &'static str,
    Vec<(&'static str, &'static str)>,
    Vec<(&'static str, String)>,
);

/// **C4: each form rule, and one schema refusal, with its field and
/// message.**
#[test]
fn c4_each_rule_refuses_with_its_field_and_message() {
    let ip_or_name = "is not an IPv4 address or a host name";
    let cases: Vec<C4Case> = vec![
        (
            "W1",
            vec![("hosts", "ch1\nch_2\nch3\nch4\nch5\nch6")],
            vec![("hosts", format!("ch_2 {ip_or_name}"))],
        ),
        (
            "W2",
            vec![("hosts", "ch1\nch1\nch3\nch4\nch5\nch6")],
            vec![("hosts", "ch1 is listed twice".into())],
        ),
        (
            "W3 shards",
            vec![("shards", "0")],
            vec![("shards", "must be a whole number from 1 to 100".into())],
        ),
        (
            "W3 replicas",
            vec![("replicas", "1.5")],
            vec![("replicas", "must be a whole number from 1 to 100".into())],
        ),
        (
            "W3 product",
            vec![("replicas", "51")],
            vec![("hosts", "102 hosts is more than 100".into())],
        ),
        (
            "W4",
            vec![("hosts", "ch1\nch2\nch3\nch4\nch5")],
            vec![("hosts", "5 hosts for 2 shards of 3 replicas: give 6".into())],
        ),
        (
            "W5",
            vec![("local_path", "var/lib/clickhouse/")],
            vec![(
                "local_path",
                "must be an absolute directory ending in /, of letters, digits, . _ / -".into(),
            )],
        ),
        (
            "W6",
            vec![("remote_url", "https://bucket.example/pulsus/?a")],
            vec![(
                "remote_url",
                "must be an http(s) URL with a host, ending in /, without a user, query or fragment"
                    .into(),
            )],
        ),
        (
            "W7 all or none",
            vec![("tls_key", "")],
            vec![(
                "tls",
                "give the certificate, the key and the CA, or none".into(),
            )],
        ),
        (
            "W7 path",
            vec![("tls_ca", "etc/ch/ca.crt")],
            vec![(
                "tls_ca",
                "must be an absolute path of letters, digits, . _ / -".into(),
            )],
        ),
        (
            "W8",
            vec![("cores", "0")],
            vec![("cores", "must be a whole number from 1 to 1000000".into())],
        ),
        (
            "W9",
            vec![("l3_width", "")],
            vec![("l4_width", "level 4 is set after an empty row".into())],
        ),
        (
            "W10 twice",
            vec![("keepers", "kp1\nkp1\nkp3")],
            vec![("keepers", "kp1 is listed twice".into())],
        ),
        (
            "W10 a ClickHouse host",
            vec![("keepers", "kp1\nkp2\nch1")],
            vec![(
                "keepers",
                "ch1 is also a ClickHouse host: Keeper runs on its own hosts".into(),
            )],
        ),
        (
            "W10 not a host",
            vec![("keepers", "kp1\nkp2\nkp_3")],
            vec![("keepers", format!("kp_3 {ip_or_name}"))],
        ),
        (
            "W11",
            vec![("keepers", "kp1\nkp2")],
            vec![("keepers", KEEPERS_REFUSED.into())],
        ),
        (
            "W12 local",
            vec![("raw_local_days", "0")],
            vec![(
                "raw_local_days",
                "must be a whole number from 1 to 4294967295".into(),
            )],
        ),
        (
            "W12 object storage",
            vec![("l1_remote", "x")],
            vec![(
                "l1_remote",
                "must be a whole number from 0 to 4294967295".into(),
            )],
        ),
        (
            "W12 no URL",
            vec![("remote_url", "")],
            vec![(
                "raw_remote_days",
                "object storage days need a remote storage URL".into(),
            )],
        ),
        (
            "W13",
            vec![("cluster", "1c")],
            vec![("cluster", "must start with a letter or _".into())],
        ),
        (
            "schema",
            vec![("l1_local", "30")],
            vec![(
                "levels",
                "PULSUS_ROLLUP_RETENTION_DAYS: 30 for the 60s level is not a whole number of \
                 days from 97 to 4294967295"
                    .into(),
            )],
        ),
    ];
    for (name, overrides, want) in cases {
        let want: Vec<(String, String)> =
            want.into_iter().map(|(f, m)| (f.to_string(), m)).collect();
        assert_eq!(refusals(&with(example(), &overrides)), Some(want), "{name}");
    }
}

/// **C5: no levels.**
#[test]
fn c5_zero_levels() {
    let files = files(&zero_levels());
    let env = lines(file(&files, "schema.env"));
    assert!(env.contains(&"PULSUS_ROLLUP_LEVELS="), "{env:?}");
    for line in &env {
        if line.starts_with("PULSUS_ROLLUP_RANGES") {
            assert_eq!(*line, "PULSUS_ROLLUP_RANGES=");
        }
        assert!(!line.starts_with("PULSUS_ROLLUP_RETENTION_DAYS"), "{line}");
        assert!(!line.starts_with("PULSUS_ROLLUP_HOT_DAYS"), "{line}");
    }
    // The finder sees the example's four level tables.
    let example_files = self::files(&example());
    let found: Vec<String> = level_tables(file(&example_files, "schema-rendered.sql"))
        .into_iter()
        .map(|(name, _)| name)
        .collect();
    assert_eq!(
        found,
        [
            "metric_samples_60s",
            "metric_samples_720s",
            "metric_samples_7200s",
            "metric_samples_86400s"
        ]
    );
    let rendered = file(&files, "schema-rendered.sql");
    assert!(!rendered.is_empty());
    assert!(
        level_tables(rendered).is_empty(),
        "{:?}",
        level_tables(rendered)
    );
}

/// Every `CREATE TABLE` statement of a level table (`metric_samples_<n>s`),
/// as (name, statement).
fn level_tables(rendered: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for stmt in rendered.split(";\n") {
        let Some(at) = stmt.find("CREATE TABLE IF NOT EXISTS ") else {
            continue;
        };
        let name: String = stmt[at + "CREATE TABLE IF NOT EXISTS ".len()..]
            .chars()
            .take_while(|c| !c.is_whitespace() && *c != '(')
            .collect();
        let bare = name.rsplit('.').next().unwrap_or(&name).trim_matches('`');
        let width = bare
            .strip_prefix("metric_samples_")
            .and_then(|rest| rest.strip_suffix('s'));
        if width.is_some_and(|w| !w.is_empty() && w.bytes().all(|b| b.is_ascii_digit())) {
            out.push((bare.to_string(), stmt.to_string()));
        }
    }
    out
}

/// **C6: one level moves; the rest stay on local disk.**
#[test]
fn c6_one_level_moves() {
    let files = files(&mixed_levels());
    let env = lines(file(&files, "schema.env"));
    assert!(
        env.contains(&"PULSUS_ROLLUP_RETENTION_DAYS=365,365,365,365"),
        "{env:?}"
    );
    assert!(
        env.contains(&"PULSUS_ROLLUP_HOT_DAYS=30,365,365,365"),
        "{env:?}"
    );
    let tables = level_tables(file(&files, "schema-rendered.sql"));
    assert!(tables.len() >= 4, "{tables:?}");
    let moving: Vec<&str> = tables
        .iter()
        .filter(|(_, stmt)| stmt.contains("TO VOLUME 'cold'"))
        .map(|(name, _)| name.as_str())
        .collect();
    assert_eq!(moving, ["metric_samples_60s"]);
}

/// **C7: no remote storage.**
#[test]
fn c7_no_remote_storage() {
    let files = files(&no_remote());
    for (path, content) in &files {
        if path.starts_with("schema/") || path == "schema-rendered.sql" {
            continue;
        }
        assert!(!content.contains("storage_configuration"), "{path}");
    }
    assert!(files.contains_key("nodes/1/config.d/pulsus-storage.xml"));
    for env in ["schema.env", "pulsusdb.env"] {
        assert!(
            !file(&files, env).contains("PULSUS_STORAGE_POLICY"),
            "{env}"
        );
    }
}

/// **C8: no TLS.**
#[test]
fn c8_no_tls() {
    let files = files(&no_tls());
    assert!(
        !files.keys().any(|p| p.ends_with("pulsus-tls.xml")),
        "{:?}",
        files.keys().collect::<Vec<_>>()
    );
    let cluster = file(&files, "nodes/1/config.d/pulsus-cluster.xml");
    assert!(cluster.contains("<port>9000</port>"), "{cluster}");
    assert!(!cluster.contains("9440"), "{cluster}");
    assert!(!cluster.contains("<secure>"), "{cluster}");
    let env = lines(file(&files, "pulsusdb.env"));
    assert!(env.contains(&"CLICKHOUSE_PROTO=http"), "{env:?}");
    assert!(env.contains(&"CLICKHOUSE_HTTP_PORT=8123"), "{env:?}");
    assert!(
        env.iter()
            .any(|l| l.starts_with("CLICKHOUSE_SERVERS=ch1:8123,")),
        "{env:?}"
    );
}

/// The text of `setup.md`'s section `### {id}`.
fn section<'a>(md: &'a str, id: &str) -> &'a str {
    let head = format!("\n### {id}\n");
    let start = md.find(&head).unwrap_or_else(|| panic!("no {id} in {md}")) + head.len();
    let rest = &md[start..];
    &rest[..rest.find("\n### ").unwrap_or(rest.len())]
}

/// The first fenced block after `marker` in `text`.
fn block_after<'a>(text: &'a str, marker: &str) -> &'a str {
    let at = text
        .find(marker)
        .unwrap_or_else(|| panic!("no {marker:?} in {text}"));
    let rest = &text[at..];
    let open = rest.find("```").expect("a fence");
    let body = &rest[open..];
    let body = &body[body.find('\n').expect("the fence's line") + 1..];
    &body[..body.find("```").expect("the closing fence")]
}

/// **C9: the expected values of the verify steps.**
#[test]
fn c9_setup_steps_expect_the_example() {
    let pairs = example();
    let files = files(&pairs);
    let md = file(&files, "setup.md");
    assert_eq!(block_after(section(md, "V1"), "Expected"), "6\n");
    let v2: Vec<&str> = block_after(section(md, "V2"), "Expected").lines().collect();
    assert_eq!(v2.len(), 12, "{v2:?}");
    assert_eq!(v2[0], "replica\tch1");
    assert_eq!(v2[5], "replica\tch6");
    assert_eq!(v2[6], "shard\t1");
    assert_eq!(v2[11], "shard\t2");
    assert_eq!(block_after(section(md, "V3"), "Expected"), "6\n");
    assert_eq!(block_after(section(md, "V4"), "Expected"), "24\n");
    let n = pulsus_schema::rendered_statements(&validated(&pairs).ctx).len();
    assert!(n > 50, "{n}");
    assert_eq!(
        block_after(section(md, "4"), "Expected"),
        format!(
            "schema.sh: all 6 hosts in cluster 'pulsus_cluster' answered\nschema.sh: {n} statements applied to pulsus\n"
        )
    );
}

const KEEPER_2: &str = "<clickhouse>
    <listen_host>::</listen_host>
    <listen_host>0.0.0.0</listen_host>
    <listen_try>1</listen_try>
    <keeper_server>
        <tcp_port_secure>9181</tcp_port_secure>
        <server_id>2</server_id>
        <log_storage_path>/var/lib/clickhouse-keeper/coordination/log</log_storage_path>
        <snapshot_storage_path>/var/lib/clickhouse-keeper/coordination/snapshots</snapshot_storage_path>
        <raft_configuration>
            <secure>true</secure>
            <server>
                <id>1</id>
                <hostname>kp1</hostname>
                <port>9234</port>
            </server>
            <server>
                <id>2</id>
                <hostname>kp2</hostname>
                <port>9234</port>
            </server>
            <server>
                <id>3</id>
                <hostname>kp3</hostname>
                <port>9234</port>
            </server>
        </raft_configuration>
    </keeper_server>
    <openSSL>
        <server>
            <certificateFile>/etc/ch/server.crt</certificateFile>
            <privateKeyFile>/etc/ch/server.key</privateKeyFile>
            <caConfig>/etc/ch/ca.crt</caConfig>
            <verificationMode>none</verificationMode>
        </server>
        <client>
            <caConfig>/etc/ch/ca.crt</caConfig>
            <verificationMode>relaxed</verificationMode>
        </client>
    </openSSL>
</clickhouse>
";

/// **C11: Keeper host 2's whole configuration.**
#[test]
fn c11_keeper_2() {
    let files = files(&example());
    assert_eq!(file(&files, "keepers/2/keeper_config.xml"), KEEPER_2);
}

const KEEPERS_REFUSED: &str = "give 1 Keeper host, or from 3 to 9";
const ONE_KEEPER: &str = "One Keeper host: no fault tolerance. If it is down, writes to \
     replicated tables and schema changes stop until it is back.";

/// `pairs`' warnings as `(field, message)`; panics when they are refused.
fn warnings(pairs: &Pairs) -> Vec<(String, String)> {
    validated(pairs)
        .warnings
        .into_iter()
        .map(|w| (w.field, w.message))
        .collect()
}

/// The one Keeper host's whole configuration: a Raft group of itself.
const KEEPER_ONE: &str = "<clickhouse>
    <listen_host>::</listen_host>
    <listen_host>0.0.0.0</listen_host>
    <listen_try>1</listen_try>
    <keeper_server>
        <tcp_port_secure>9181</tcp_port_secure>
        <server_id>1</server_id>
        <log_storage_path>/var/lib/clickhouse-keeper/coordination/log</log_storage_path>
        <snapshot_storage_path>/var/lib/clickhouse-keeper/coordination/snapshots</snapshot_storage_path>
        <raft_configuration>
            <secure>true</secure>
            <server>
                <id>1</id>
                <hostname>kp1</hostname>
                <port>9234</port>
            </server>
        </raft_configuration>
    </keeper_server>
    <openSSL>
        <server>
            <certificateFile>/etc/ch/server.crt</certificateFile>
            <privateKeyFile>/etc/ch/server.key</privateKeyFile>
            <caConfig>/etc/ch/ca.crt</caConfig>
            <verificationMode>none</verificationMode>
        </server>
        <client>
            <caConfig>/etc/ch/ca.crt</caConfig>
            <verificationMode>relaxed</verificationMode>
        </client>
    </openSSL>
</clickhouse>
";

/// **C12: 1 Keeper host, or from 3 to 9, none a ClickHouse host.** One is
/// accepted with a warning on `keepers`; 2 and 10 are refused; 3 and 4
/// are accepted with none.
#[test]
fn c12_keeper_count_and_overlap() {
    let one = with(example(), &[("keepers", "kp1")]);
    assert!(refusals(&one).is_none(), "{:?}", refusals(&one));
    assert_eq!(
        warnings(&one),
        [("keepers".to_string(), ONE_KEEPER.to_string())]
    );
    for refused in [
        "kp1\nkp2",
        "kp1\nkp2\nkp3\nkp4\nkp5\nkp6\nkp7\nkp8\nkp9\nkp10",
    ] {
        assert_eq!(
            field_messages(&with(example(), &[("keepers", refused)]), "keepers"),
            [KEEPERS_REFUSED],
            "{refused:?}"
        );
    }
    let three = example();
    assert!(warnings(&three).is_empty(), "{:?}", warnings(&three));
    let four = with(example(), &[("keepers", "kp1\nkp2\nkp3\nkp4")]);
    assert!(refusals(&four).is_none(), "{:?}", refusals(&four));
    assert!(warnings(&four).is_empty(), "{:?}", warnings(&four));
    assert_eq!(
        files(&four)
            .keys()
            .filter(|p| p.starts_with("keepers/"))
            .count(),
        4
    );
    let overlap = with(example(), &[("keepers", "kp1\nkp2\nch1")]);
    assert_eq!(
        field_messages(&overlap, "keepers"),
        ["ch1 is also a ClickHouse host: Keeper runs on its own hosts"]
    );
}

/// **C12b: a bundle for 1 Keeper host.** One Keeper file, holding a Raft
/// group of exactly one member; each server's config names that one
/// Keeper; `setup.md` says "1 Keeper host" and carries the warning, which
/// a 3-Keeper `setup.md` does not.
#[test]
fn c12b_one_keeper_bundle() {
    let one = files(&with(example(), &[("keepers", "kp1")]));
    let keeper_files: Vec<&String> = one.keys().filter(|p| p.starts_with("keepers/")).collect();
    assert_eq!(keeper_files, ["keepers/1/keeper_config.xml"]);
    let keeper = file(&one, "keepers/1/keeper_config.xml");
    assert_eq!(
        keeper.matches("<server>\n                <id>").count(),
        1,
        "{keeper}"
    );
    assert_eq!(keeper, KEEPER_ONE);
    let cluster = file(&one, "nodes/1/config.d/pulsus-cluster.xml");
    let zookeeper = &cluster[cluster.find("<zookeeper>").expect("a zookeeper block")
        ..cluster.find("</zookeeper>").expect("the block closes")];
    assert_eq!(zookeeper.matches("<node>").count(), 1, "{zookeeper}");
    assert!(zookeeper.contains("<host>kp1</host>"), "{zookeeper}");
    let md = file(&one, "setup.md");
    assert!(md.contains(", 1 Keeper host, "), "{md}");
    assert!(md.contains(ONE_KEEPER), "{md}");
    let three = files(&example());
    let md = file(&three, "setup.md");
    assert!(md.contains(", 3 Keeper hosts, "), "{md}");
    assert!(!md.contains("no fault tolerance"), "{md}");
}

/// **C15: no memory, thread or cache setting is written.**
#[test]
fn c15_no_memory_or_thread_settings() {
    let forbidden = [
        "max_server_memory_usage",
        "max_memory_usage",
        "max_threads",
        "background_",
        "_cache_size",
        "max_concurrent_queries",
        "merges_",
    ];
    for (name, pairs) in [
        ("example", example()),
        ("zero levels", zero_levels()),
        ("mixed", mixed_levels()),
        ("no remote", no_remote()),
        ("no tls", no_tls()),
    ] {
        let files = files(&pairs);
        assert!(files.len() > 10, "{name}");
        for (path, content) in &files {
            if path == "schema-rendered.sql" || path.starts_with("schema/") {
                continue;
            }
            for word in forbidden {
                assert!(!content.contains(word), "{name}: {path} holds {word}");
            }
        }
    }
}

fn hosts(n: usize) -> String {
    (1..=n)
        .map(|i| format!("h{i}"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// **C16: each rule's bounds.**
#[test]
fn c16_bounds() {
    let whole_100 = "must be a whole number from 1 to 100";
    let whole_1m = "must be a whole number from 1 to 1000000";
    let whole_days = "must be a whole number from 1 to 4294967295";
    let whole_days_0 = "must be a whole number from 0 to 4294967295";
    let digits25 = "1234567890123456789012345";

    // W3.
    let s100 = with(
        example(),
        &[("shards", "100"), ("replicas", "1"), ("hosts", &hosts(100))],
    );
    assert!(refusals(&s100).is_none(), "{:?}", refusals(&s100));
    let ten_by_ten = with(
        example(),
        &[("shards", "10"), ("replicas", "10"), ("hosts", &hosts(100))],
    );
    assert!(
        refusals(&ten_by_ten).is_none(),
        "{:?}",
        refusals(&ten_by_ten)
    );
    for field in ["shards", "replicas"] {
        for bad in ["101", digits25] {
            assert_eq!(
                field_messages(&with(example(), &[(field, bad)]), field),
                [whole_100],
                "{field} {bad}"
            );
        }
    }
    assert_eq!(
        field_messages(&with(example(), &[("replicas", "51")]), "hosts"),
        ["102 hosts is more than 100"]
    );
    assert_eq!(
        field_messages(
            &with(
                example(),
                &[("shards", "100"), ("replicas", "100"), ("hosts", "a\nb\nc")]
            ),
            "hosts"
        ),
        ["10000 hosts is more than 100"]
    );

    // W8.
    for field in ["cores", "memory_gib", "disk_gib"] {
        assert!(
            field_messages(&with(example(), &[(field, "1000000")]), field).is_empty(),
            "{field}"
        );
        for bad in ["1000001", digits25] {
            assert_eq!(
                field_messages(&with(example(), &[(field, bad)]), field),
                [whole_1m],
                "{field} {bad}"
            );
        }
    }

    // W12: the field's own bound; the total then goes to the schema.
    let max_raw = with(
        zero_levels(),
        &[("raw_local_days", "4294967295"), ("raw_remote_days", "0")],
    );
    assert!(refusals(&max_raw).is_none(), "{:?}", refusals(&max_raw));
    for field in ["raw_local_days", "l1_local"] {
        assert!(
            field_messages(&with(example(), &[(field, "4294967295")]), field).is_empty(),
            "{field}"
        );
        for bad in ["4294967296", digits25] {
            assert_eq!(
                field_messages(&with(example(), &[(field, bad)]), field),
                [whole_days],
                "{field} {bad}"
            );
        }
    }
    for field in ["raw_remote_days", "l1_remote"] {
        assert!(
            field_messages(&with(example(), &[(field, "4294967295")]), field).is_empty(),
            "{field}"
        );
        for bad in ["4294967296", digits25] {
            assert_eq!(
                field_messages(&with(example(), &[(field, bad)]), field),
                [whole_days_0],
                "{field} {bad}"
            );
        }
    }
    // A total over the schema's bound is the schema's refusal.
    assert_eq!(
        refusals(&with(
            zero_levels(),
            &[("raw_local_days", "4294967295"), ("raw_remote_days", "1")]
        )),
        Some(vec![(
            "raw".to_string(),
            "PULSUS_RETENTION_DAYS must be a whole number of days from 1 to 4294967295: 4294967296"
                .to_string()
        )])
    );

    // W11.
    let nine = (1..=9)
        .map(|i| format!("kp{i}"))
        .collect::<Vec<_>>()
        .join("\n");
    let ten = (1..=10)
        .map(|i| format!("kp{i}"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(field_messages(&with(example(), &[("keepers", &nine)]), "keepers").is_empty());
    assert_eq!(
        field_messages(&with(example(), &[("keepers", &ten)]), "keepers"),
        [KEEPERS_REFUSED]
    );

    // W2 and W10 after lowercasing.
    assert_eq!(
        field_messages(
            &with(example(), &[("hosts", "ch1\nCH1\nch3\nch4\nch5\nch6")]),
            "hosts"
        ),
        ["ch1 is listed twice"]
    );
    assert_eq!(
        field_messages(&with(example(), &[("keepers", "kp1\nkp2\nCH1")]), "keepers"),
        ["ch1 is also a ClickHouse host: Keeper runs on its own hosts"]
    );
    // Lowercased in every output.
    let upper = with(example(), &[("hosts", "CH1\nch2\nch3\nch4\nch5\nch6")]);
    let upper_files = files(&upper);
    assert!(
        file(&upper_files, "nodes/1/config.d/pulsus-node.xml").contains("<replica>ch1</replica>")
    );

    // W5 and W7.
    for bad in ["/var/lib/\n", "/var/li b/", "/var/lib$/"] {
        assert_eq!(
            field_messages(&with(example(), &[("local_path", bad)]), "local_path"),
            ["must be an absolute directory ending in /, of letters, digits, . _ / -"],
            "{bad:?}"
        );
    }
    for bad in ["/etc/ch/a\n", "/etc/ch/a b", "/etc/ch/$a"] {
        assert_eq!(
            field_messages(&with(example(), &[("tls_cert", bad)]), "tls_cert"),
            ["must be an absolute path of letters, digits, . _ / -"],
            "{bad:?}"
        );
    }

    // W6.
    for bad in [
        "https://u:p@h/",
        "https:///",
        "https://h/?a",
        "https://h/#a",
        "https://h",
        "ftp://h/",
    ] {
        assert_eq!(
            field_messages(&with(example(), &[("remote_url", bad)]), "remote_url"),
            ["must be an http(s) URL with a host, ending in /, without a user, query or fragment"],
            "{bad:?}"
        );
    }
    assert!(
        field_messages(
            &with(example(), &[("remote_url", "http://10.0.0.1:9000/b/")]),
            "remote_url"
        )
        .is_empty()
    );

    // W13.
    assert_eq!(
        field_messages(&with(example(), &[("cluster", "1c")]), "cluster"),
        ["must start with a letter or _"]
    );
    for good in ["_c", "c1"] {
        let pairs = with(example(), &[("cluster", good)]);
        assert!(refusals(&pairs).is_none(), "{good}: {:?}", refusals(&pairs));
    }

    // W1.
    let label64 = "a".repeat(64);
    for bad in [
        ".",
        "..",
        "a..b",
        "-a",
        "a-",
        label64.as_str(),
        "::1",
        "a_b",
    ] {
        let list = format!("{bad}\nch2\nch3\nch4\nch5\nch6");
        assert_eq!(
            field_messages(&with(example(), &[("hosts", &list)]), "hosts"),
            [format!(
                "{} is not an IPv4 address or a host name",
                bad.to_ascii_lowercase()
            )],
            "{bad:?}"
        );
    }
    let label63 = "a".repeat(63);
    for good in ["10.0.0.1", label63.as_str(), "a-b.example"] {
        let list = format!("{good}\nch2\nch3\nch4\nch5\nch6");
        assert!(
            field_messages(&with(example(), &[("hosts", &list)]), "hosts").is_empty(),
            "{good}"
        );
    }
}

/// A path under the build's temporary directory, outside the source tree,
/// that does not exist yet: a bundle's destination.
fn fresh_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pulsus-setup-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// **C17: the bundle carries the script and the schema, and the script
/// run from the bundle prints `schema-rendered.sql`.**
#[test]
fn c17_the_bundle_runs_its_own_script() {
    let files = files(&example());
    let dir = fresh_dir("c17");
    let list: Vec<pulsus_setup::File> = files
        .iter()
        .map(|(path, content)| pulsus_setup::File {
            path: path.clone(),
            content: content.clone(),
        })
        .collect();
    write_bundle(&dir, &list).expect("write the bundle");
    for path in ["schema/schema.sh", "schema/schema.sql"] {
        let written = std::fs::read(dir.join(path)).unwrap_or_else(|e| panic!("{path}: {e}"));
        let repo = std::fs::read(repo_root().join(path)).expect("the repository's file");
        assert!(written == repo, "{path} differs from the repository's");
    }
    let (code, out, err) = script_print(&dir, file(&files, "schema.env"));
    assert_eq!(code, Some(0), "{err}");
    assert_eq!(out, file(&files, "schema-rendered.sql"));
    let _ = std::fs::remove_dir_all(&dir);
}

/// **Issue #746: the admin pair's default.** Step 5 says the admin pair is
/// `admin` / `admin` until set and to set the password; V5 reads the admin
/// API with the pair from the environment, else the default.
#[test]
fn the_steps_name_the_default_admin_pair() {
    let files = files(&example());
    let md = file(&files, "setup.md");
    let step = section(md, "5");
    assert!(
        step.contains("`admin` / `admin`") && step.contains("set `PULSUS_ADMIN_PASSWORD`"),
        "{step}"
    );
    let v5 = section(md, "V5");
    assert!(
        v5.contains(r#"-u "${PULSUS_ADMIN_USER:-admin}:${PULSUS_ADMIN_PASSWORD:-admin}""#),
        "{v5}"
    );
}

/// The `sh` block of `setup.md`'s step 4.
fn step4_command(md: &str) -> String {
    let section = section(md, "4");
    let at = section.find("```sh\n").expect("step 4's command") + "```sh\n".len();
    section[at..at + section[at..].find("```").expect("the fence closes")].to_string()
}

/// **Review r1 finding 1: step 4 stops, naming the password variable, when
/// it is not set in the shell that runs it.**
#[test]
fn step4_refuses_to_run_without_the_password() {
    let files = files(&example());
    let md = file(&files, "setup.md");
    assert!(
        section(md, "4").contains("PULSUS_CLICKHOUSE_PASSWORD"),
        "step 4 says where the password comes from"
    );
    let dir = fresh_dir("step4");
    let list: Vec<pulsus_setup::File> = files
        .iter()
        .map(|(path, content)| pulsus_setup::File {
            path: path.clone(),
            content: content.clone(),
        })
        .collect();
    write_bundle(&dir, &list).expect("write the bundle");
    let out = Command::new("sh")
        .arg("-c")
        .arg(step4_command(md))
        .current_dir(&dir)
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("PULSUS_SCHEMA_WAIT_SECONDS", "1")
        .output()
        .expect("run sh");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "{stderr}");
    assert!(stderr.contains("PULSUS_CLICKHOUSE_PASSWORD"), "{stderr}");
    assert!(
        !stderr.contains("did not answer"),
        "it stops before the server: {stderr}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// **Review r1 finding 2: a bundle is not written over an earlier one,
/// whose files the new one may not have.**
#[test]
fn a_bundle_is_not_written_over_another() {
    let dir = fresh_dir("over");
    let tls = bundle(&validated(&example()));
    write_bundle(&dir, &tls).expect("the first bundle");
    assert!(dir.join("nodes/1/config.d/pulsus-tls.xml").exists());
    let plain = bundle(&validated(&no_tls()));
    let err = write_bundle(&dir, &plain).expect_err("the directory holds a bundle");
    assert!(err.to_string().contains("already exists"), "{err}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// **Review r1 finding 3: the object-storage URL's whole authority is
/// checked, its port included.**
#[test]
fn the_remote_url_authority_is_checked() {
    let message =
        "must be an http(s) URL with a host, ending in /, without a user, query or fragment";
    for bad in [
        "https://h:99999/b/",
        "https://h:0/b/",
        "https://h:65536/b/",
        "https://h:/b/",
        "https://h:x/b/",
        "https://[::1]:99999/b/",
    ] {
        assert_eq!(
            field_messages(&with(example(), &[("remote_url", bad)]), "remote_url"),
            [message],
            "{bad:?}"
        );
    }
    // Review r2 finding 2: any parsed host, bracketed IPv6 included.
    for good in [
        "https://h:65535/b/",
        "http://10.0.0.1:9000/b/",
        "https://s3.example/b/",
        "https://[::1]:9000/b/",
        "http://[2001:db8::1]/b/",
    ] {
        assert!(
            field_messages(&with(example(), &[("remote_url", good)]), "remote_url").is_empty(),
            "{good:?}"
        );
    }
}

/// **Review r1 finding 5: raw data with no object-storage days, under the
/// tiered policy, is described as the levels are: no move rule, local
/// while the disk has room.**
#[test]
fn raw_data_with_no_remote_days_has_no_move_rule() {
    let lines = pulsus_setup::total_lines(&validated(&with(example(), &[("raw_remote_days", "")])));
    assert_eq!(
        lines[0],
        "Raw data: kept at least 7 days; no move rule; on local disk while the local disk has room"
    );
    let local_only = pulsus_setup::total_lines(&validated(&no_remote()));
    assert_eq!(
        local_only[0],
        "Raw data: kept at least 7 days; at least 7 on local disk"
    );
}

/// **Review r2 finding 1: a destination that already exists is refused,
/// even empty, and one that cannot be read is refused without a file
/// written.**
#[test]
fn an_existing_destination_is_refused() {
    let files = bundle(&validated(&example()));
    let empty = fresh_dir("exists-empty");
    std::fs::create_dir_all(&empty).unwrap();
    let err = write_bundle(&empty, &files).expect_err("the directory exists");
    assert!(err.to_string().contains("already exists"), "{err}");
    assert_eq!(std::fs::read_dir(&empty).unwrap().count(), 0);
    let _ = std::fs::remove_dir_all(&empty);

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let unreadable = fresh_dir("exists-unreadable");
        std::fs::create_dir_all(&unreadable).unwrap();
        std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o300)).unwrap();
        let result = write_bundle(&unreadable, &files);
        std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(result.is_err(), "an unreadable destination is refused");
        assert_eq!(
            std::fs::read_dir(&unreadable).unwrap().count(),
            0,
            "nothing written"
        );
        let _ = std::fs::remove_dir_all(&unreadable);
    }
}

/// Every file under `dir`, by its path inside it, sorted.
fn files_under(dir: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else {
                out.push(
                    path.strip_prefix(dir)
                        .unwrap()
                        .to_string_lossy()
                        .to_string(),
                );
            }
        }
    }
    out.sort();
    out
}

/// **Review r2 finding 1: two bundles written to one destination at once
/// never mix: one is written whole, the other refused, and nothing is
/// left beside the destination.**
#[test]
fn two_bundles_at_once_never_mix() {
    let tls = bundle(&validated(&example()));
    let plain = bundle(&validated(&no_tls()));
    for round in 0..20 {
        let dir = fresh_dir(&format!("race-{round}"));
        let barrier = std::sync::Barrier::new(2);
        let (a, b) = std::thread::scope(|s| {
            let one = s.spawn(|| {
                barrier.wait();
                write_bundle(&dir, &tls)
            });
            let two = s.spawn(|| {
                barrier.wait();
                write_bundle(&dir, &plain)
            });
            (one.join().unwrap(), two.join().unwrap())
        });
        assert!(
            a.is_ok() != b.is_ok(),
            "round {round}: exactly one is written: {a:?} {b:?}"
        );
        let won = if a.is_ok() { &tls } else { &plain };
        let mut want: Vec<String> = won.iter().map(|f| f.path.clone()).collect();
        want.sort();
        assert_eq!(
            files_under(&dir),
            want,
            "round {round}: the winner's files and no other"
        );
        let _ = std::fs::remove_dir_all(&dir);
        let name = dir.file_name().unwrap().to_string_lossy().to_string();
        let stray: Vec<String> = std::fs::read_dir(dir.parent().unwrap())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|n| n.starts_with(&format!(".{name}")))
            .collect();
        assert!(stray.is_empty(), "round {round}: left beside it: {stray:?}");
    }
}

/// **Review r3: a destination whose parent directories do not exist yet is
/// written, its parents created.**
#[test]
fn a_nested_new_destination_is_written() {
    let root = fresh_dir("nested");
    let dir = root.join("a/b/bundle");
    let files = bundle(&validated(&example()));
    write_bundle(&dir, &files).expect("the parents are created");
    let mut want: Vec<String> = files.iter().map(|f| f.path.clone()).collect();
    want.sort();
    assert_eq!(files_under(&dir), want);
    let _ = std::fs::remove_dir_all(&root);
}

/// Every variable the bundle tells a host to set from a secret store.
const SECRET_VARS: [&str; 6] = [
    "CLICKHOUSE_INTERSERVER_SECRET",
    "PULSUS_CLICKHOUSE_PASSWORD",
    "PULSUS_S3_ACCESS_KEY_ID",
    "PULSUS_S3_SECRET_ACCESS_KEY",
    "CLICKHOUSE_AUTH",
    "PULSUS_ADMIN_PASSWORD",
];

/// Each value `text` assigns to `name`, as `name=value` (an env file) or
/// `name: value` (a Kubernetes Secret), to the end of its line.
fn assigned<'a>(text: &'a str, name: &str) -> Vec<&'a str> {
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.trim_start();
        for sep in ["=", ": "] {
            if let Some(value) = line.strip_prefix(&format!("{name}{sep}")) {
                out.push(value);
            }
        }
    }
    out
}

/// **Step 2 says how to set each ClickHouse host's variables**, by
/// systemd, Docker and Kubernetes, naming the object store's two only
/// when there is a remote URL; step 5 does the same for PulsusDB's.
#[test]
fn the_steps_say_how_to_set_the_variables() {
    let ch_always = [
        "CLICKHOUSE_INTERSERVER_SECRET",
        "PULSUS_CLICKHOUSE_PASSWORD",
    ];
    let s3 = ["PULSUS_S3_ACCESS_KEY_ID", "PULSUS_S3_SECRET_ACCESS_KEY"];
    for (name, pairs, with_s3) in [
        ("remote", example(), true),
        ("no remote", no_remote(), false),
    ] {
        let files = files(&pairs);
        let md = file(&files, "setup.md");
        for id in ["2", "5"] {
            let step = section(md, id);
            for needle in [
                "How to set them",
                "install -m 600",
                "systemctl edit",
                "EnvironmentFile=",
                "systemctl restart",
                "--env-file",
                "env_file:",
                "kind: Secret",
                "envFrom:",
            ] {
                assert!(
                    step.contains(needle),
                    "{name}: step {id} lacks {needle:?}: {step}"
                );
            }
        }
        let step2 = section(md, "2");
        for var in ch_always {
            assert_eq!(assigned(step2, var).len(), 2, "{name}: {var}: {step2}");
        }
        for var in s3 {
            let want = if with_s3 { 2 } else { 0 };
            assert_eq!(assigned(step2, var).len(), want, "{name}: {var}: {step2}");
            if !with_s3 {
                assert!(!step2.contains(var), "{name}: {var}: {step2}");
            }
        }
        let step5 = section(md, "5");
        for var in ["CLICKHOUSE_AUTH", "PULSUS_ADMIN_PASSWORD"] {
            assert_eq!(assigned(step5, var).len(), 2, "{name}: {var}: {step5}");
        }
        assert!(!step5.contains("PULSUS_S3_"), "{name}: {step5}");
    }
}

/// **No secret value is in any file**: every value the bundle gives a
/// secret variable is a `<…>` placeholder or a reference to the shell's
/// own variable, and each is given at least once, so the scan has
/// something to read.
#[test]
fn no_file_holds_a_secret_value() {
    let placeholder = |v: &str| {
        let v = v.trim_matches('"');
        v.starts_with('<') || v.starts_with("pulsus:<") || v == "pulsus:$PULSUS_CLICKHOUSE_PASSWORD"
    };
    let files = files(&example());
    for var in SECRET_VARS {
        let mut seen = 0;
        for (path, content) in &files {
            for value in assigned(content, var) {
                seen += 1;
                assert!(placeholder(value), "{path}: {var} is given {value:?}");
            }
        }
        assert!(seen >= 2, "{var} is given {seen} times");
    }
}

/// `setup.md`'s part `## {title}`, to the next `## ` heading.
fn part<'a>(md: &'a str, title: &str) -> &'a str {
    let head = format!("\n## {title}\n");
    let start = md
        .find(&head)
        .unwrap_or_else(|| panic!("no {title} in {md}"))
        + head.len();
    let rest = &md[start..];
    &rest[..rest.find("\n## ").unwrap_or(rest.len())]
}

/// The row for `path` in setup.md's "Where each file goes" table, as
/// (host, destination), the destination without its backquotes.
fn destination(md: &str, path: &str) -> Option<(String, String)> {
    let table = part(md, "Where each file goes");
    let head = format!("| `{path}` | ");
    let rows: Vec<&str> = table.lines().filter(|l| l.starts_with(&head)).collect();
    assert!(rows.len() <= 1, "{path} has {} rows", rows.len());
    let cells: Vec<&str> = rows.first()?.split(" | ").collect();
    Some((cells[1].to_string(), cells[2].trim_matches('`').to_string()))
}

/// **Every file in the bundle has its destination in setup.md**: the
/// host and the exact path, checked against this test's own mapping.
#[test]
fn setup_md_says_where_every_file_goes() {
    let pairs = example();
    let v = validated(&pairs);
    let files = files(&pairs);
    let md = file(&files, "setup.md");
    let step4 = "the machine that runs step 4";
    for path in files.keys() {
        let want = if let Some(rest) = path.strip_prefix("nodes/") {
            let (i, rest) = rest.split_once('/').unwrap();
            let host = v.hosts[i.parse::<usize>().unwrap() - 1].clone();
            (host, format!("/etc/clickhouse-server/{rest}"))
        } else if let Some(rest) = path.strip_prefix("keepers/") {
            let (j, _) = rest.split_once('/').unwrap();
            let host = v.keepers[j.parse::<usize>().unwrap() - 1].clone();
            (host, "/etc/clickhouse-keeper/keeper_config.xml".to_string())
        } else if path == "pulsusdb.env" {
            (
                "the PulsusDB host".to_string(),
                "/etc/pulsusdb/pulsusdb.env".to_string(),
            )
        } else if [
            "schema.env",
            "schema-rendered.sql",
            "schema/schema.sh",
            "schema/schema.sql",
            "setup.md",
        ]
        .contains(&path.as_str())
        {
            (step4.to_string(), format!("pulsus-bundle/{path}"))
        } else {
            panic!("this test has no destination for {path}");
        };
        assert_eq!(destination(md, path), Some(want), "{path}");
    }
}

/// **Each step gives the copy commands, owners and modes.**
#[test]
fn the_steps_give_the_copy_commands() {
    let pairs = example();
    let v = validated(&pairs);
    let bundle_files = files(&pairs);
    let md = file(&bundle_files, "setup.md");
    let bundle = part(md, "The bundle");
    for needle in ["pulsus-bundle/", "/admin/api/v1/setup/bundle", "Download"] {
        assert!(bundle.contains(needle), "{needle:?}: {bundle}");
    }

    let k1 = section(md, "K1");
    for (j, kp) in v.keepers.iter().enumerate() {
        let scp = format!("scp keepers/{}/keeper_config.xml {kp}:", j + 1);
        assert!(k1.contains(&scp), "{scp}: {k1}");
    }
    for needle in [
        "-o clickhouse -g clickhouse",
        "/etc/clickhouse-keeper/keeper_config.xml",
        "/var/lib/clickhouse-keeper/coordination/log",
        "/var/lib/clickhouse-keeper/coordination/snapshots",
        "systemctl enable --now clickhouse-keeper",
        "/etc/ch/server.crt",
        "/etc/ch/server.key",
        "/etc/ch/ca.crt",
    ] {
        assert!(k1.contains(needle), "{needle:?}: {k1}");
    }

    let step1 = section(md, "1");
    for (i, host) in v.hosts.iter().enumerate() {
        let scp = format!("scp -r nodes/{} {host}:", i + 1);
        assert!(step1.contains(&scp), "{scp}: {step1}");
    }
    for needle in [
        "/etc/clickhouse-server/config.d/",
        "/etc/clickhouse-server/users.d/",
        "-m 640 -o clickhouse -g clickhouse",
        "install -d -m 750 -o clickhouse -g clickhouse /var/lib/clickhouse/",
        "/etc/ch/server.crt",
        "/etc/ch/server.key",
        "/etc/ch/ca.crt",
    ] {
        assert!(step1.contains(needle), "{needle:?}: {step1}");
    }

    assert!(
        section(md, "4").contains("pulsus-bundle"),
        "{}",
        section(md, "4")
    );
    let step5 = section(md, "5");
    assert!(
        step5
            .contains("install -m 640 -o root -g pulsusdb pulsusdb.env /etc/pulsusdb/pulsusdb.env"),
        "{step5}"
    );
    let containers = part(md, "In containers");
    for needle in [
        "/etc/clickhouse-server/config.d/",
        "/etc/clickhouse-server/users.d/",
        "/etc/clickhouse-keeper/keeper_config.xml",
        "/var/lib/clickhouse-keeper",
        "/var/lib/clickhouse/",
        "--env-file pulsusdb.env",
        "Kubernetes",
    ] {
        assert!(containers.contains(needle), "{needle:?}: {containers}");
    }

    let plain = files(&no_tls());
    let md = file(&plain, "setup.md");
    assert!(
        !section(md, "1").contains("/etc/ch/"),
        "{}",
        section(md, "1")
    );
    assert!(
        !section(md, "K1").contains("/etc/ch/"),
        "{}",
        section(md, "K1")
    );
}

/// **Each file's destination, as the page and the API show it**: `host:/path`
/// for a file placed on a host, `<pulsusdb host>:` for `pulsusdb.env`,
/// and where it is used for the rest; each agrees with setup.md's table.
#[test]
fn each_file_has_a_destination_label() {
    let pairs = example();
    let v = validated(&pairs);
    let files = files(&pairs);
    let label = |path: &str| pulsus_setup::destination_label(&v, path);
    assert_eq!(
        label("nodes/1/config.d/pulsus-cluster.xml"),
        "ch1:/etc/clickhouse-server/config.d/pulsus-cluster.xml"
    );
    assert_eq!(
        label("nodes/6/users.d/pulsus-user.xml"),
        "ch6:/etc/clickhouse-server/users.d/pulsus-user.xml"
    );
    assert_eq!(
        label("keepers/2/keeper_config.xml"),
        "kp2:/etc/clickhouse-keeper/keeper_config.xml"
    );
    assert_eq!(
        label("pulsusdb.env"),
        "<pulsusdb host>:/etc/pulsusdb/pulsusdb.env"
    );
    assert_eq!(
        label("schema.env"),
        "on the machine you run step 4 from: pulsus-bundle/schema.env"
    );
    assert_eq!(
        label("schema/schema.sh"),
        "on the machine you run step 4 from: pulsus-bundle/schema/schema.sh"
    );
    let md = file(&files, "setup.md");
    for path in files.keys() {
        let (host, dest) = destination(md, path).unwrap_or_else(|| panic!("{path}"));
        let got = label(path);
        let want = if v.hosts.contains(&host) || v.keepers.contains(&host) {
            format!("{host}:{dest}")
        } else if path == "pulsusdb.env" {
            format!("<pulsusdb host>:{dest}")
        } else {
            format!("on the machine you run step 4 from: {dest}")
        };
        assert_eq!(got, want, "{path}");
    }
}
