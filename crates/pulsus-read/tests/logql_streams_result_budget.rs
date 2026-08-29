//! Issue #312 — the streams retention ledger, driven through the SHIPPED
//! retention paths rather than through transcriptions of them.
//!
//! `MAX_LIMIT` (5,000) bounds a streams response's ENTRIES and nothing
//! bounded its BYTES: the enforceable figures were `5,000 × 64 MiB` =
//! 320 GiB staged on the non-dropping path and `50,000 × 64 MiB` =
//! 3.1 TiB for one page of the dropping path, because bytes per row are
//! bounded only by the 64 MiB decompressed ingest cap. Every test here
//! goes through `StreamsFastPathProbe` / `StreamAccumulator` /
//! `StreamsPagedProbe`, each of which is the production body behind a
//! `#[doc(hidden)]` seam.
//!
//! **Sizes are computed, never materialised** — the derivation rows below
//! reach 1 GiB and a 64 MiB line, and no test allocates either.

use std::collections::HashMap;

use pulsus_read::logql::exec::{
    STREAM_FEED_CHUNK_BYTES, StreamAccumulator, StreamsFastPathProbe, StreamsPagedProbe,
};
use pulsus_read::logql::pipeline::CompiledPipeline;
use pulsus_read::logql::rows::{SampleRow, StreamMetaRow, TailSampleRow};
use pulsus_read::logql::{
    MAX_STREAMS_RESULT_BYTES, PlanCtx, QueryParams, QuerySpec, ReadError, StreamResult,
    TooBroadReason, plan,
};

// ---------------------------------------------------------------------
// The charge model, transcribed from `logql::charge` so this suite can
// state a footprint independently of the ledger it is checking. Charge
// helpers are `pub(crate)` there on purpose (they are not an API), so an
// out-of-crate conservation test has to carry its own copy — which is
// what makes AC-2 a real cross-check rather than the ledger agreeing
// with itself.
//
//   alloc_block_bytes(n) = max(2n, 32)      charge.rs
//   grown_alloc_bytes(n) = 3 × alloc_block_bytes(n)
//   map_entry_bytes(w)   = (w + 1) × 8 + 128
// ---------------------------------------------------------------------

const MIN_ALLOC_BYTES: u64 = 32;
/// `size_of::<(i64, String)>()`.
const STREAM_ENTRY_SLOT: u64 = 32;
/// `max(size_of::<(u64, StreamResult)>(), size_of::<(String, FanOutGroup)>())`.
///
/// 88 -> 112 with issue #463: `StreamResult` gained a `categories`
/// `Vec` (24 B) and `FanOutGroup` gained the same, so both shapes grew
/// by one `Vec` and the production constant — a `size_of` — moved with
/// them. This is a per-STREAM widening, not per-entry.
const STREAM_GROUP_SLOT: u64 = 112;
/// `size_of::<SampleRow>()`.
const STAGED_ROW_SLOT: u64 = 64;

fn alloc_block_bytes(n: u64) -> u64 {
    (n * 2).max(MIN_ALLOC_BYTES)
}

fn grown_alloc_bytes(n: u64) -> u64 {
    alloc_block_bytes(n) * 3
}

fn map_entry_bytes(slot: u64) -> u64 {
    (slot + 1) * 8 + 128
}

fn entry_bytes(line_len: u64) -> u64 {
    alloc_block_bytes(line_len) + STREAM_ENTRY_SLOT
}

fn group_bytes(labels_json_len: u64, service_len: u64) -> u64 {
    map_entry_bytes(STREAM_GROUP_SLOT)
        + grown_alloc_bytes(labels_json_len)
        + alloc_block_bytes(service_len)
}

/// EVERYTHING one staged `SampleRow` retains — body AND structured
/// metadata AND the slot. Charging the body alone under-prices a
/// conforming push by 65.4× (issue #312 §1a).
fn staged_row_bytes(body_len: u64, sm_len: u64) -> u64 {
    alloc_block_bytes(body_len) + alloc_block_bytes(sm_len) + STAGED_ROW_SLOT
}

/// The footprint of a RETURNED result, recomputed from the streams that
/// came back.
fn result_footprint(streams: &[StreamResult]) -> u64 {
    streams
        .iter()
        .map(|s| {
            group_bytes(s.labels_json.len() as u64, s.service.len() as u64)
                + s.entries
                    .iter()
                    .map(|(_, line)| entry_bytes(line.len() as u64))
                    .sum::<u64>()
        })
        .sum()
}

// ---------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------

const SVC: &str = "svc";
const LABELS: &str = r#"{"service_name":"svc"}"#;

fn meta_one() -> HashMap<u64, StreamMetaRow> {
    HashMap::from([(
        1u64,
        StreamMetaRow {
            fingerprint: 1,
            service: SVC.to_string(),
            labels: LABELS.to_string(),
        },
    )])
}

fn compiled(query: &str) -> CompiledPipeline {
    let expr = pulsus_logql::parse(query).expect("parse");
    let pulsus_logql::Expr::Log(log) = expr else {
        panic!("expected a log query: {query}");
    };
    CompiledPipeline::compile(&log.pipeline).expect("compile")
}

fn row(i: usize, body_len: usize, sm: &str) -> SampleRow {
    SampleRow {
        fingerprint: 1,
        timestamp_ns: 1_700_000_000_000_000_000i64 + i as i64,
        body: "x".repeat(body_len),
        structured_metadata: sm.to_string(),
    }
}

fn tail_row(i: usize, body_len: usize) -> TailSampleRow {
    TailSampleRow {
        fingerprint: 1,
        timestamp_ns: 1_700_000_000_000_000_000i64 + i as i64,
        body: "x".repeat(body_len),
        body_hash: i as u64,
        structured_metadata: String::new(),
    }
}

fn assert_streams_result_bytes(err: &ReadError, expected_cap: u64) {
    match err {
        ReadError::QueryTooBroad(TooBroadReason::StreamsResultBytes { bytes, cap }) => {
            assert_eq!(
                *cap, expected_cap,
                "the refusal must name the cap the path was given"
            );
            assert!(
                *bytes > *cap,
                "a refusal must report the peak that breached: {bytes} vs {cap}"
            );
        }
        other => panic!("expected StreamsResultBytes, got {other:?}"),
    }
}

// ---------------------------------------------------------------------
// AC-1 — all four response paths refuse past the cap
// ---------------------------------------------------------------------

/// AC-1. Each of the four streams response paths refuses, with the cap it
/// was given, and none of them truncates instead.
#[test]
fn every_streams_response_path_refuses_past_the_cap() {
    const CAP: u64 = 100_000;
    const BODY: usize = 4_096;
    let meta = meta_one();
    let pipeline = compiled(r#"{a="b"} |= "x""#);

    // (a) the streamed fast path — zero-structured-metadata rows.
    let mut probe = StreamsFastPathProbe::with_cap(CAP);
    let err = (0..1_000)
        .find_map(|i| probe.push_row(row(i, BODY, ""), &meta).err())
        .expect("the fast path must refuse before 1,000 rows at a 100 KB cap");
    assert_streams_result_bytes(&err, CAP);

    // (a') the same path's SM-bearing sub-case (`SmFanOutAccumulator`),
    // whose rows fan out per entry rather than grouping by fingerprint.
    let mut probe = StreamsFastPathProbe::with_cap(CAP);
    let err = (0..1_000)
        .find_map(|i| {
            probe
                .push_row(row(i, BODY, &format!(r#"{{"trace":"t{i}"}}"#)), &meta)
                .err()
        })
        .expect("the SM fan-out must refuse before 1,000 rows at a 100 KB cap");
    assert_streams_result_bytes(&err, CAP);

    // (b) the non-dropping path, in its PRODUCTION shape: drain into
    // `push_row`, then one `flush_chunk` after the drain.
    let mut acc = StreamAccumulator::with_cap(&meta, u32::MAX, CAP);
    let err = (0..1_000)
        .find_map(|i| acc.push_row(row(i, BODY, ""), &pipeline).err())
        .or_else(|| acc.flush_chunk(&pipeline).err())
        .expect("the accumulator must refuse before 1,000 rows at a 100 KB cap");
    assert_streams_result_bytes(&err, CAP);

    // (c) the paged path, over an injected stream through the REAL
    // `StreamsPagedState::absorb_page`.
    let mut acc = StreamAccumulator::with_cap(&meta, u32::MAX, CAP);
    let mut st = StreamsPagedProbe::new(1_000, u64::MAX);
    let rows: Vec<Result<TailSampleRow, ReadError>> =
        (0..1_000).map(|i| Ok(tail_row(i, BODY))).collect();
    let mut stream = futures::stream::iter(rows);
    let err = futures::executor::block_on(st.absorb_page(&mut stream, 1, &mut acc, &pipeline))
        .expect_err("the paged loop must refuse, not return a partial");
    assert_streams_result_bytes(&err, CAP);

    // (d) the tail-poll shape: one fresh accumulator per poll, so the
    // budget is PER POLL (issue #312 risk 5) — the poll that breaches
    // fails, and a following poll starts from zero.
    let mut poll = StreamAccumulator::with_cap(&meta, u32::MAX, CAP);
    let err = (0..1_000)
        .find_map(|i| poll.push_row(row(i, BODY, ""), &pipeline).err())
        .or_else(|| poll.flush_chunk(&pipeline).err())
        .expect("a tail poll over the cap must fail that poll");
    assert_streams_result_bytes(&err, CAP);
    let mut next_poll = StreamAccumulator::with_cap(&meta, u32::MAX, CAP);
    next_poll
        .push_row(row(0, BODY, ""), &pipeline)
        .expect("the next poll starts from an empty ledger");
    next_poll.flush_chunk(&pipeline).expect("still empty");
    assert!(next_poll.charged() > 0 && next_poll.charged() < CAP);
}

/// AC-1's scope check: the branch set the sentence above names is the
/// branch set `run_streams_inner` has. Its three branches are decided by
/// exactly two predicates — `CompiledPipeline::is_line_filter_only()` and
/// `StreamsPlan::fetch_until_limit` — and the fourth path is a different
/// ENTRY POINT (`tail_poll`), not a fourth branch.
#[test]
fn the_four_refusing_paths_are_the_whole_branch_set() {
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: 1_700_000_000_000_000_000,
            end_ns: 1_700_000_060_000_000_000,
            step_ns: 1_000_000_000,
        },
        limit: 100,
        direction: pulsus_read::logql::Direction::Backward,
    };
    let ctx = PlanCtx {
        db: "pulsus",
        streams_idx: "log_streams_idx",
        streams: "log_streams",
        samples: "log_samples",
        rollup_table: "log_metrics_5s",
        rollup_res_ns: 5_000_000_000,
        scan_budget_bytes: 50 * 1024 * 1024 * 1024,
        max_streams: 100_000,
        pipeline_scan_factor: 10,
    };
    let mut seen: Vec<(bool, bool)> = Vec::new();
    for query in [
        // fast path: a line filter only, fully pushed down.
        r#"{a="b"} |= "x""#,
        // non-dropping transform/fan-out: a parser, nothing drops.
        r#"{a="b"} | logfmt"#,
        // dropping: an in-engine label filter, so fetch-until-limit pages.
        r#"{a="b"} | logfmt | lvl = "error""#,
    ] {
        let expr = pulsus_logql::parse(query).expect("parse");
        let pulsus_read::logql::Plan::Streams(sp) = plan(&expr, &params, &ctx).expect("plan")
        else {
            panic!("expected a Streams plan for {query}");
        };
        let c = CompiledPipeline::compile(&sp.pipeline).expect("compile");
        seen.push((c.is_line_filter_only(), sp.fetch_until_limit));
    }
    assert_eq!(
        seen,
        vec![(true, false), (false, false), (false, true)],
        "run_streams_inner's branch predicates moved — the four paths AC-1 drives are no \
         longer the whole set"
    );
}

// ---------------------------------------------------------------------
// AC-2 — the conservation identity
// ---------------------------------------------------------------------

/// AC-2. `charged == footprint(result)` EXACTLY after every path, with
/// `staged() == 0`. Equality, not `<=`: a `<=` would not catch an
/// UNDER-charge that never trips the cap, which is the failure that
/// silently reopens the hole.
#[test]
fn the_ledger_equals_what_came_back() {
    let meta = meta_one();

    // The streamed fast path, both sub-cases at once.
    let mut probe = StreamsFastPathProbe::with_cap(MAX_STREAMS_RESULT_BYTES);
    for i in 0..200 {
        let sm = if i % 3 == 0 {
            format!(r#"{{"trace":"t{}"}}"#, i % 7)
        } else {
            String::new()
        };
        probe
            .push_row(row(i, 40 + i, &sm), &meta)
            .expect("admitted");
    }
    let charged = probe.charged();
    assert_eq!(probe.staged(), 0, "the fast path stages nothing at all");
    let streams = probe.into_streams();
    assert!(!streams.is_empty());
    assert_eq!(
        charged,
        result_footprint(&streams),
        "the fast-path ledger and the retained footprint must agree exactly"
    );

    // The accumulator, transform and fan-out.
    for query in [r#"{a="b"} |= "x""#, r#"{a="b"} | logfmt"#] {
        let pipeline = compiled(query);
        let mut acc = StreamAccumulator::with_cap(&meta, u32::MAX, MAX_STREAMS_RESULT_BYTES);
        for i in 0..200 {
            acc.push_row(
                SampleRow {
                    fingerprint: 1,
                    timestamp_ns: 1_700_000_000_000_000_000i64 + i as i64,
                    body: format!("lvl=info seq={i} msg=xxxxxxxxxxxxxxxxxxxx"),
                    structured_metadata: String::new(),
                },
                &pipeline,
            )
            .expect("admitted");
        }
        acc.flush_chunk(&pipeline).expect("admitted");
        let charged = acc.charged();
        assert_eq!(acc.staged(), 0, "{query}: every staged charge is released");
        let streams = acc.into_streams();
        assert!(
            !streams.is_empty(),
            "{query}: the fixture must produce output"
        );
        assert_eq!(
            charged,
            result_footprint(&streams),
            "{query}: the ledger and the retained footprint must agree exactly"
        );
    }
}

// ---------------------------------------------------------------------
// AC-3 — staged bytes are bounded by the CHUNK, not by the stream
// ---------------------------------------------------------------------

/// AC-3. Peak staged bytes over a 20,000-row stream stay inside one chunk
/// plus at most one row — the whole point of denominating the chunk in
/// BYTES. Flushing on a row count instead pushes the peak to 329,600,000.
#[test]
fn staged_bytes_are_bounded_by_the_chunk() {
    const ROWS: usize = 20_000;
    const BODY: usize = 8 * 1024;
    let meta = meta_one();
    let pipeline = compiled(r#"{a="b"} |= "x""#);
    let mut acc = StreamAccumulator::with_cap(&meta, u32::MAX, u64::MAX);

    let footprint = staged_row_bytes(BODY as u64, 0);
    let ceiling = STREAM_FEED_CHUNK_BYTES + footprint;
    let whole_stream = footprint * ROWS as u64;

    let mut peak = 0u64;
    for i in 0..ROWS {
        acc.push_row(row(i, BODY, ""), &pipeline).expect("admitted");
        peak = peak.max(acc.staged());
    }
    acc.flush_chunk(&pipeline).expect("admitted");

    assert!(
        peak <= ceiling,
        "peak staged bytes {peak} exceeded the chunk ceiling {ceiling} over a {ROWS}-row \
         stream (whole-stream staging would be {whole_stream})"
    );
    assert_eq!(
        acc.staged(),
        0,
        "every staged charge must be released once its chunk is fed"
    );
    // Non-vacuous: whole-stream staging is 39× the ceiling, so a
    // row-count flush cannot pass this by accident.
    assert!(whole_stream > ceiling * 30);
}

// ---------------------------------------------------------------------
// AC-5 — the whole retained row is charged, not just the line
// ---------------------------------------------------------------------

/// AC-5. A CONFORMING push (structured metadata inside the 64 KiB ingest
/// ceiling `MAX_STRUCTURED_METADATA_BYTES_PER_ENTRY`) whose SM dwarfs its
/// body must be refused at a cap set to 4× the line-ONLY price. Dropping
/// the `structured_metadata` term from the staged charge admits it.
#[test]
fn sm_staging_is_charged_for_the_whole_row() {
    const ROWS: usize = 200;
    const BODY: usize = 1024;
    // 65,012 B of SM — a conforming push, not an adversarial one.
    let sm = format!(r#"{{"k":"{}"}}"#, "v".repeat(65_000));
    assert!(sm.len() < 64 * 1024 + 1024, "the fixture stays conforming");

    let line_only_price = ROWS as u64 * entry_bytes(BODY as u64);
    let cap = line_only_price * 4;
    let retained = ROWS as u64 * staged_row_bytes(BODY as u64, sm.len() as u64);
    // The measured 65.4× under-pricing, restated as an inequality this
    // fixture depends on.
    assert!(
        retained > cap * 4,
        "the fixture must be far past a 4× line-only cap: retained {retained} vs cap {cap}"
    );

    let meta = meta_one();
    let pipeline = compiled(r#"{a="b"} |= "x""#);
    let mut acc = StreamAccumulator::with_cap(&meta, u32::MAX, cap);
    let err = (0..ROWS)
        // The PRODUCTION shape: drain into `push_row`, then one
        // `flush_chunk` after the drain — never a flush per row, which
        // would discharge the staging this test is about.
        .find_map(|i| acc.push_row(row(i, BODY, &sm), &pipeline).err())
        .or_else(|| acc.flush_chunk(&pipeline).err())
        .unwrap_or_else(|| {
            panic!(
                "staging {ROWS} rows of {BODY} line bytes + {} SM bytes was ADMITTED at a cap \
                 of {cap} (4x the line-only price {line_only_price}) — the ledger is not \
                 charging what the row retains",
                sm.len()
            )
        });
    assert_streams_result_bytes(&err, cap);
}

// ---------------------------------------------------------------------
// AC-6 — refusal, never truncation; and the mid-page prefix
// ---------------------------------------------------------------------

/// AC-6. At the boundary, `N` entries succeed and `N + 1` returns `Err`;
/// the `Ok` result carries exactly `N`, and the paged case returns `Err`
/// rather than `Ok((streams, true))` — a refusal is never downgraded to a
/// partial and is never a truncation.
#[test]
fn a_breach_is_never_a_partial_or_a_truncation() {
    const BODY: usize = 4_096;
    let meta = meta_one();
    let pipeline = compiled(r#"{a="b"} |= "x""#);

    // The cap is EXACTLY the footprint of N entries in one group, so the
    // N+1-th is the first refusal. The fast path stages nothing, so the
    // cap carries no staging term.
    const N: u64 = 8;
    let cap = group_bytes(LABELS.len() as u64, SVC.len() as u64) + N * entry_bytes(BODY as u64);

    let mut probe = StreamsFastPathProbe::with_cap(cap);
    for i in 0..N as usize {
        probe
            .push_row(row(i, BODY, ""), &meta)
            .unwrap_or_else(|e| panic!("entry {i} of {N} must be admitted: {e}"));
    }
    let err = probe
        .push_row(row(N as usize, BODY, ""), &meta)
        .expect_err("the N+1-th entry must be refused");
    assert_streams_result_bytes(&err, cap);
    let streams = probe.into_streams();
    let entries: usize = streams.iter().map(|s| s.entries.len()).sum();
    assert_eq!(
        entries as u64, N,
        "the refused entry must not appear in the result"
    );

    // The paged case: `Err`, not `Ok((streams, true))`.
    let mut acc = StreamAccumulator::with_cap(&meta, u32::MAX, cap);
    let mut st = StreamsPagedProbe::new(64, u64::MAX).with_spent(1_000_000);
    let rows: Vec<Result<TailSampleRow, ReadError>> =
        (0..64).map(|i| Ok(tail_row(i, BODY))).collect();
    let mut stream = futures::stream::iter(rows);
    let out = futures::executor::block_on(st.absorb_page(&mut stream, 1, &mut acc, &pipeline));
    let err = out.expect_err(
        "a result-budget breach on a LATER page must propagate, not be downgraded to a partial",
    );
    assert_streams_result_bytes(&err, cap);
}

/// AC-6, second half — the #244 prefix precedent, both ways. A
/// `ScanBudgetBytes` breach mid-page keeps the rows that already arrived
/// when a previous page has been read (`spent > 0`); on the FIRST page
/// (`spent == 0`) it stays the #90 `QueryTooBroad` 422 regardless of how
/// many rows were already delivered.
#[test]
fn a_mid_page_scan_budget_breach_keeps_the_prefix_and_the_first_page_422() {
    let meta = meta_one();
    let pipeline = compiled(r#"{a="b"} |= "x""#);
    let budget_err = || {
        ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes {
            budget_bytes: 1_000,
            estimate: None,
        })
    };
    let page = || -> Vec<Result<TailSampleRow, ReadError>> {
        let mut v: Vec<Result<TailSampleRow, ReadError>> =
            (0..10).map(|i| Ok(tail_row(i, 64))).collect();
        v.push(Err(budget_err()));
        v
    };

    // Later page (`spent > 0`): terminate-PARTIAL, prefix retained.
    let mut acc = StreamAccumulator::new(&meta, u32::MAX);
    let mut st = StreamsPagedProbe::new(64, u64::MAX).with_spent(4_096);
    let mut stream = futures::stream::iter(page());
    let decision = futures::executor::block_on(st.absorb_page(&mut stream, 1, &mut acc, &pipeline))
        .expect("a later-page budget breach is a partial, not an error");
    assert_eq!(decision, Some(true), "terminate-PARTIAL");
    let entries: usize = acc.into_streams().iter().map(|s| s.entries.len()).sum();
    assert_eq!(
        entries, 10,
        "the ten rows delivered before the breach must be kept — the prefix boundary is not \
         required to align with a page boundary (issue #244's ruled precedent)"
    );

    // First page (`spent == 0`): the 422 stands.
    let mut acc = StreamAccumulator::new(&meta, u32::MAX);
    let mut st = StreamsPagedProbe::new(64, u64::MAX);
    let mut stream = futures::stream::iter(page());
    let err = futures::executor::block_on(st.absorb_page(&mut stream, 1, &mut acc, &pipeline))
        .expect_err("a first-page budget breach stays a 422");
    assert!(
        matches!(
            err,
            ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes { .. })
        ),
        "expected the #90 first-page error, got {err:?}"
    );
}

// ---------------------------------------------------------------------
// AC-7 — the derivation rows
// ---------------------------------------------------------------------

/// AC-7. The four rows `MAX_STREAMS_RESULT_BYTES`' doc table publishes,
/// computed rather than materialised. A/B/C are admitted and D is
/// refused, so lowering the constant below row B or raising it past row D
/// fails here.
#[test]
fn the_derivation_rows_are_admitted_and_row_d_is_refused() {
    /// `entries × (lines) + groups × (labels) + one chunk of staging`.
    fn total(entries: u64, line: u64, groups: u64, labels: u64, staged_line: u64) -> u64 {
        entries * entry_bytes(line)
            + groups * group_bytes(labels, SVC.len() as u64)
            + STREAM_FEED_CHUNK_BYTES
            + staged_row_bytes(staged_line, 0)
    }

    // Row C is ONE maximal ingestible line, so its staging term is that
    // one row rather than a full chunk.
    let one_max_line = 64 * 1024 * 1024u64;
    let row_c = entry_bytes(one_max_line)
        + group_bytes(64, SVC.len() as u64)
        + staged_row_bytes(one_max_line, 0);

    let rows: [(&str, u64, bool); 4] = [
        (
            "A: 5,000 × 64 KiB lines, 5,000 streams, 1 KiB labels_json",
            total(5_000, 65_536, 5_000, 1_024, 65_536),
            true,
        ),
        (
            "B: 5,000 × 100 KiB lines, 1 stream, 64 B labels_json",
            total(5_000, 102_400, 1, 64, 102_400),
            true,
        ),
        ("C: ONE maximal ingestible line (64 MiB body)", row_c, true),
        (
            "D: 5,000 × 100 KiB lines, 5,000 streams, 2 KiB labels_json",
            total(5_000, 102_400, 5_000, 2_048, 102_400),
            false,
        ),
    ];

    // The exact figures the doc table publishes, so a slot width moving
    // reddens here as well as the admit/refuse verdict.
    assert_eq!(rows[0].1, 700_079_776);
    assert_eq!(rows[1].1, 1_032_754_952);
    assert_eq!(rows[2].1, 268_437_032);
    assert_eq!(rows[3].1, 1_099_513_504);

    for (name, total, admitted) in rows {
        assert_eq!(
            total <= MAX_STREAMS_RESULT_BYTES,
            admitted,
            "row {name}: {total} B against the {MAX_STREAMS_RESULT_BYTES} B cap"
        );
    }

    // Row C's property, stated on its own because it is the one that
    // matters most: no single stored row is ever unreturnable.
    assert!(
        row_c < MAX_STREAMS_RESULT_BYTES,
        "a maximal ingestible line must always be returnable"
    );
}

// ---------------------------------------------------------------------
// AC-18 — the cap documents its 3× conservatism
// ---------------------------------------------------------------------

/// AC-18. `MAX_STREAMS_RESULT_BYTES`' doc block states the wide-label
/// conservatism with its number, read back from the committed source; and
/// the number is `MAX_STREAMS_RESULT_BYTES / 3`, so it cannot drift from
/// the constant either. Deleting the line fails this.
#[test]
fn the_cap_documents_its_3x_conservatism() {
    let src = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/logql/charge.rs"),
    )
    .expect("charge.rs readable");
    let doc = src
        .split("pub const MAX_STREAMS_RESULT_BYTES")
        .next()
        .expect("charge.rs declares MAX_STREAMS_RESULT_BYTES");
    // The constant's own doc block: everything after the last blank line
    // above the declaration.
    let doc = doc.rsplit("\n\n").next().expect("a doc block");
    assert!(
        doc.contains("3x") || doc.contains("3×"),
        "MAX_STREAMS_RESULT_BYTES' doc does not state the 3x wide-label conservatism"
    );
    let effective = MAX_STREAMS_RESULT_BYTES / 3;
    let spelled = {
        let mut n = effective;
        let mut parts = Vec::new();
        while n >= 1_000 {
            parts.push(format!("{:03}", n % 1_000));
            n /= 1_000;
        }
        parts.push(n.to_string());
        parts.reverse();
        parts.join(",")
    };
    assert_eq!(spelled, "357,913,941");
    assert!(
        doc.contains(&spelled),
        "MAX_STREAMS_RESULT_BYTES' doc does not quote the effective wide-label ceiling \
         {spelled} — a reader hitting a refusal at a third of the stated cap would file it \
         as a bug"
    );
}

// ---------------------------------------------------------------------
// AC-16 — the ledger entry exists (a PRESENCE check, and said to be one)
// ---------------------------------------------------------------------

/// AC-16. The differential ledger carries the `streams-result-budget`
/// entry reproducing the round-1 reference capture.
///
/// This is a **presence** check and nothing more: it establishes that the
/// entry and its measured figures are on disk, NOT that the measurement
/// table is honest. Its provenance is issue #312 comment `5265167134`'s
/// captured table — a live run against `grafana/loki:3.7.4`, recorded
/// rather than re-derived here.
#[test]
fn the_ledger_records_the_measured_reference_behaviour() {
    let ledger = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../docs/benchmarks/logs-differential-ledger.md"),
    )
    .expect("ledger readable");
    let entry = ledger
        .split("### `streams-result-budget` (issue #312, bounded divergence)")
        .nth(1)
        .expect("the ledger carries the streams-result-budget entry");
    for token in [
        "4,102,645",   // the largest response the reference SERVED
        "4194304",     // the two gRPC message ceilings
        "13113586",    // the verbatim 500 body's figure
        "504",         // the 60-second hang class
        "line_format", // the amplification shape
    ] {
        assert!(
            entry.contains(token),
            "the streams-result-budget entry no longer records {token}"
        );
    }
}

// ---------------------------------------------------------------------
// Issue #463 — the CATEGORISED shape's bytes are charged
// ---------------------------------------------------------------------

/// The same rows, categorised and not, through the SAME shipped
/// accumulator: the categorised ledger must exceed the plain one by at
/// least the metadata bytes the third element retains.
///
/// "At least the metadata bytes" is the floor the plan states and it is
/// the load-bearing half — a renderer that emits a third element the
/// budget did not price is exactly how the 1 GiB retention cap stops
/// bounding the result. The actual excess is larger, because each entry
/// also retains two `Vec` spines and the `EntryCategories` struct itself.
#[test]
fn the_categorised_shape_charges_its_third_element() {
    let meta = meta_one();
    // 200 rows, each carrying one 64-byte ordinary metadata pair. The
    // pair's key and value are what the third element retains.
    let value: String = "v".repeat(64);
    let sm = format!(r#"{{"trace_id":"{value}"}}"#);

    let mut plain = StreamsFastPathProbe::with_cap(MAX_STREAMS_RESULT_BYTES);
    let mut categorised = StreamsFastPathProbe::with_cap_categorized(MAX_STREAMS_RESULT_BYTES);
    for i in 0..200 {
        let r = |body_len: usize| SampleRow {
            fingerprint: 1,
            timestamp_ns: 1_700_000_000_000_000_000i64 + i as i64,
            body: "b".repeat(body_len),
            structured_metadata: sm.clone(),
        };
        plain.push_row(r(40), &meta).expect("admitted");
        categorised.push_row(r(40), &meta).expect("admitted");
    }

    let metadata_bytes =
        200 * (alloc_block_bytes(b"trace_id".len() as u64) + alloc_block_bytes(value.len() as u64));
    let excess = categorised.charged() - plain.charged();
    assert!(
        excess >= metadata_bytes,
        "the categorised ledger exceeds the plain one by {excess} B, which is less than the \
         {metadata_bytes} B of metadata the third element retains"
    );

    // And the two shapes really are the same rows: one categorised
    // stream against 200 plain ones, because the categorised path groups
    // by the STREAM-category subset while the plain path fans a
    // metadata-bearing row into its own merged label set.
    assert_eq!(categorised.into_streams().len(), 1);
    assert_eq!(plain.into_streams().len(), 1);
}

/// A categorised query whose METADATA alone exceeds the cap is refused
/// with the named `422`, never truncated — the same complete-or-error
/// class every other retention breach takes.
///
/// The discriminator is that the same rows WITHOUT the header are
/// admitted under the same cap: what refuses this query is the third
/// element's bytes and nothing else.
#[test]
fn a_categorised_query_whose_metadata_alone_exceeds_the_cap_is_refused() {
    let meta = meta_one();
    let value: String = "v".repeat(4_096);
    let sm = format!(r#"{{"trace_id":"{value}"}}"#);
    let row = |i: usize| SampleRow {
        fingerprint: 1,
        timestamp_ns: 1_700_000_000_000_000_000i64 + i as i64,
        body: "b".repeat(8),
        structured_metadata: sm.clone(),
    };

    // A cap that admits the plain shape's whole retention and nothing
    // more: measured from the plain probe itself, so the number is not
    // hand-derived.
    let mut sizing = StreamsFastPathProbe::with_cap(MAX_STREAMS_RESULT_BYTES);
    for i in 0..50 {
        sizing.push_row(row(i), &meta).expect("admitted");
    }
    let cap = sizing.charged();

    let mut plain = StreamsFastPathProbe::with_cap(cap);
    for i in 0..50 {
        plain
            .push_row(row(i), &meta)
            .expect("the plain shape fits its own footprint exactly");
    }

    let mut categorised = StreamsFastPathProbe::with_cap_categorized(cap);
    let mut refusal = None;
    for i in 0..50 {
        if let Err(e) = categorised.push_row(row(i), &meta) {
            refusal = Some(e);
            break;
        }
    }
    let err = refusal.expect("the categorised shape must not fit the plain shape's cap");
    assert!(
        matches!(
            err,
            ReadError::QueryTooBroad(TooBroadReason::StreamsResultBytes { .. })
        ),
        "expected the named streams-result refusal, got {err:?}"
    );
}
