//! P1 of issue #819: the JFR parse's time against the model's step on the
//! same push. A measurement, not a gate: run by hand in release,
//!
//! `cargo test --release -p pulsus-write --test profileql_jfr_cost -- --ignored --nocapture`
//!
//! and compare the two medians it prints. A parse slower than the model
//! step is a defect to raise.

use std::io::Read as _;
use std::time::{Duration, Instant};

use pulsus_write::parse_profiles;
use pulsus_write::protocols::profile_push::parse_legacy_query;
use pulsus_write::protocols::profileql_legacy::jfr::{LabelsSnapshot, jfr_inputs, parse_jfr};

const RUNS: usize = 50;

fn example() -> Vec<u8> {
    let gz = include_bytes!("fixtures/profileql_jfr/example.jfr.gz");
    let mut out = Vec::new();
    flate2::read::MultiGzDecoder::new(&gz[..])
        .read_to_end(&mut out)
        .expect("a gzip file");
    out
}

fn median(mut runs: Vec<Duration>) -> Duration {
    runs.sort();
    runs[runs.len() / 2]
}

#[test]
#[ignore = "P1: a measurement, run by hand in release"]
fn p1_the_jfr_parse_against_the_model_step() {
    let body = example();
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_nanos() as i64;
    let now_ms = now_ns / 1_000_000;
    let labels = LabelsSnapshot::default();

    let mut parse = Vec::with_capacity(RUNS);
    for _ in 0..RUNS {
        let t = Instant::now();
        let out = parse_jfr(&body, &labels, now_ns, 10_000_000_000, 100).expect("parses");
        parse.push(t.elapsed());
        assert_eq!(out.profiles.len(), 4);
    }

    let query = parse_legacy_query(Some("name=checkout&format=jfr&sampleRate=100"), now_ns)
        .expect("a valid query");
    let inputs = jfr_inputs(&query, "", &body, now_ns).expect("parses");
    let mut model = Vec::with_capacity(RUNS);
    for _ in 0..RUNS {
        let copy = inputs.clone();
        let t = Instant::now();
        let landing = parse_profiles(copy, now_ms).expect("the model takes it");
        model.push(t.elapsed());
        assert!(!landing.is_empty());
    }

    println!(
        "P1 example.jfr ({} bytes), median of {RUNS}: parse_jfr {} us, parse_profiles {} us",
        body.len(),
        median(parse).as_micros(),
        median(model).as_micros()
    );
}
