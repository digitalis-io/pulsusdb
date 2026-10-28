//! `schema/schema.sh` over TLS (issue #746 part D): tests D-1 and D-6. The
//! script against a TLS server is D-4 and D-5, in
//! `crates/pulsus-server/tests/tls_trust.rs`, which has the certificate
//! tools.

use std::path::{Path, PathBuf};
use std::process::Command;

fn script_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../schema/schema.sh")
}

/// **D-1: a scheme other than http or https is refused, before anything
/// is printed.**
#[test]
fn d1_the_script_refuses_an_unknown_scheme() {
    for (proto, code, message) in [
        (
            "ftp",
            Some(2),
            Some("schema.sh: CLICKHOUSE_PROTO must be http or https: ftp\n"),
        ),
        (
            "HTTPS",
            Some(2),
            Some("schema.sh: CLICKHOUSE_PROTO must be http or https: HTTPS\n"),
        ),
        ("https", Some(0), None),
        ("http", Some(0), None),
        ("", Some(0), None),
    ] {
        let out = Command::new("sh")
            .arg(script_path())
            .arg("--print")
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("PULSUS_RETENTION_DAYS", "7")
            .env("CLICKHOUSE_PROTO", proto)
            .output()
            .expect("run sh");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert_eq!(out.status.code(), code, "{proto:?}: {stderr}");
        if let Some(message) = message {
            assert_eq!(stderr, message, "{proto:?}");
            assert!(out.stdout.is_empty(), "{proto:?}");
        }
    }
}

/// **D-6: every request the script sends carries the CA option, and none
/// skips verification.**
#[test]
fn d6_every_curl_carries_the_ca_option() {
    let script = std::fs::read_to_string(script_path()).expect("schema.sh");
    let mut lines_with_curl = 0;
    let mut invocations = 0;
    for line in script.lines() {
        let code = line.trim_start();
        if code.starts_with('#') {
            continue;
        }
        let here = code.matches("curl ").count();
        if here == 0 {
            continue;
        }
        lines_with_curl += 1;
        invocations += here;
        assert!(code.contains("$cacert"), "no CA option: {line}");
        assert!(
            !code.contains(" -k") && !code.contains("--insecure"),
            "skips verification: {line}"
        );
    }
    assert!(lines_with_curl >= 3, "found {lines_with_curl}");
    assert_eq!(invocations, lines_with_curl, "one curl per line");
}
