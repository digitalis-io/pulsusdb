//! Issue #605: the stored `detected_level` is bounded by the rule, not by the
//! fixture. 1,000 distinct syslog-shaped lines with no level field and no
//! severity text store at most the eight level names on every transport; the
//! same lines sent with a per-line OTLP severity text store that text, one
//! distinct value per line, as the reference does (captured case o13).

use std::collections::{BTreeMap, BTreeSet};

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use serde_json::Value as Json;

use pulsus_write::protocols::loki_push::{EntryAdapter, PushRequest, StreamAdapter, Timestamp};
use pulsus_write::protocols::otlp_logs;
use pulsus_write::{
    LevelDiscovery, LogIngestSettings, ParsedLogs, parse_loki_json, parse_loki_protobuf,
};

const N: usize = 1000;
const TS_NS: i64 = 1_788_099_000_000_000_000;
const HEADER: &str = "2026-09-25T07:41:09.123456+00:00 host1 ";
const LEVEL_NAMES: [&str; 8] = [
    "trace", "debug", "info", "warn", "error", "critical", "fatal", "unknown",
];

/// The eight lines of issue #605's table; `{i}` makes every line distinct.
/// The expected level is the issue's third column, derived by hand from the
/// reference's rule, not from this code.
fn template(i: usize) -> (String, &'static str) {
    match i % 8 {
        0 => (
            format!("systemd[{i}]: update-notifier-download.service: Deactivated successfully."),
            "unknown",
        ),
        1 => (
            format!(
                "systemd[{i}]: Finished update-notifier-download.service - Download data for packages that failed at package install time."
            ),
            "unknown",
        ),
        2 => (
            format!(
                "systemd[{i}]: Starting update-notifier-download.service - Download data for packages that failed at package install time..."
            ),
            "unknown",
        ),
        3 => (
            format!(
                "cassandra[{i}]: INFO  [IndexSummaryManager:1] 2026-09-25T07:41:09,854 IndexSummaryRedistribution.java:154 - Completed resizing of index summaries"
            ),
            "info",
        ),
        4 => (
            format!("gnome-shell[{i}]: Errors from xkbcomp are not fatal to the X server"),
            "fatal",
        ),
        5 => (
            format!(
                "gnome-shell[{i}]: meta_window_set_stack_position_no_sync: assertion 'window->stack_position >= 0' failed"
            ),
            "unknown",
        ),
        6 => (
            format!("systemd[{i}]: mpris-proxy.service: Failed with result 'exit-code'."),
            "unknown",
        ),
        _ => (
            format!(
                "kernel: audit: type=1400 audit(1790322158.977:{i}): apparmor=\"DENIED\" operation=\"capable\" class=\"cap\" profile=\"fusermount3\" pid={i} comm=\"fusermount3\" capability=7  capname=\"setuid\""
            ),
            "unknown",
        ),
    }
}

fn level_by_body(out: &ParsedLogs) -> BTreeMap<String, String> {
    out.rows
        .iter()
        .map(|r| {
            let sm: Json =
                serde_json::from_str(&r.structured_metadata).expect("stored metadata is JSON");
            (
                r.body.clone(),
                sm["detected_level"]
                    .as_str()
                    .expect("detected_level present")
                    .to_string(),
            )
        })
        .collect()
}

fn json_push() -> ParsedLogs {
    let values: Vec<Json> = (0..N)
        .map(|i| {
            serde_json::json!([
                (TS_NS + i as i64).to_string(),
                format!("{HEADER}{}", template(i).0)
            ])
        })
        .collect();
    let body =
        serde_json::json!({"streams": [{"stream": {"service_name": "s605"}, "values": values}]});
    parse_loki_json(body.to_string().as_bytes(), 0, LevelDiscovery::On).expect("json push accepted")
}

fn protobuf_push() -> ParsedLogs {
    let entries = (0..N)
        .map(|i| {
            let ts = TS_NS + i as i64;
            EntryAdapter {
                timestamp: Some(Timestamp {
                    seconds: ts / 1_000_000_000,
                    nanos: (ts % 1_000_000_000) as i32,
                }),
                line: format!("{HEADER}{}", template(i).0),
                structured_metadata: Vec::new(),
            }
        })
        .collect();
    let req = PushRequest {
        streams: vec![StreamAdapter {
            labels: r#"{service_name="s605"}"#.to_string(),
            entries,
        }],
    };
    parse_loki_protobuf(&req, 0, LevelDiscovery::On).expect("protobuf push accepted")
}

/// `severity_text(i)` is the record's OTLP severity text; severity number 0.
fn otlp(severity_text: impl Fn(usize) -> String) -> ParsedLogs {
    let str_value = |s: &str| {
        Some(AnyValue {
            value: Some(Value::StringValue(s.to_string())),
        })
    };
    let records = (0..N)
        .map(|i| LogRecord {
            time_unix_nano: (TS_NS + i as i64) as u64,
            severity_text: severity_text(i),
            body: str_value(&format!("{HEADER}{}", template(i).0)),
            ..Default::default()
        })
        .collect();
    let req = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: str_value("s605"),
                    key_strindex: 0,
                }],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope::default()),
                log_records: records,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    otlp_logs::parse(
        &req,
        0,
        LogIngestSettings {
            discover_log_levels: true,
        },
    )
    .expect("otlp accepted")
}

#[test]
fn a_line_with_no_level_field_stores_one_of_the_eight_level_names_on_every_transport() {
    for (transport, out) in [
        ("json", json_push()),
        ("protobuf", protobuf_push()),
        ("otlp", otlp(|_| String::new())),
    ] {
        let got = level_by_body(&out);
        assert_eq!(got.len(), N, "{transport}: {N} distinct stored lines");
        let mut wrong = Vec::new();
        for i in 0..N {
            let (line, want) = template(i);
            let have = &got[&format!("{HEADER}{line}")];
            if have != want {
                wrong.push(format!("line {i}: want {want:?}, stored {have:?}"));
            }
        }
        assert!(
            wrong.is_empty(),
            "{transport}: {} wrong:\n{}",
            wrong.len(),
            wrong.join("\n")
        );
        let distinct: BTreeSet<&str> = got.values().map(String::as_str).collect();
        assert!(
            distinct.iter().all(|v| LEVEL_NAMES.contains(v)),
            "{transport}: {distinct:?}"
        );
        assert_eq!(
            distinct,
            BTreeSet::from(["fatal", "info", "unknown"]),
            "{transport}"
        );
    }
}

#[test]
fn an_otlp_severity_text_that_is_not_a_level_word_is_stored_as_sent() {
    let text = |i: usize| template(i).0;
    let got = level_by_body(&otlp(text));
    let mut wrong = Vec::new();
    for i in 0..N {
        let have = &got[&format!("{HEADER}{}", template(i).0)];
        if *have != text(i) {
            wrong.push(format!("line {i}: stored {have:?}"));
        }
    }
    assert!(
        wrong.is_empty(),
        "{} wrong:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
    let distinct: BTreeSet<&String> = got.values().collect();
    assert_eq!(
        distinct.len(),
        N,
        "one stored level per distinct severity text"
    );
}
