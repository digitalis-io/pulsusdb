//! The RSS-probe fresh-child protocol (architect plan v5 [R1]): a
//! parent-side windowed `VmRSS` sampler, split out of `paths.rs` for size
//! (clippy has no line-count lint, but this module's own guideline is
//! <1000 lines/file). Parent side: [`run_rss_probe_parent`]/
//! [`run_one_rss_probe`] spawn a fresh child per RSS repetition, handshake
//! `READY`/go-signal/`DONE`, and poll `/proc/<child_pid>/status` `VmRSS`
//! every 10 ms from the go-signal to `DONE`, attributing `rss_peak -
//! rss_at_ready`. Child side: [`run_rss_probe_child`] connects to the
//! already-loaded database, confirms it is ready, runs exactly one
//! [`super::paths::run_variant_once`] call inside the handshake window, and
//! self-reports its own `VmHWM(exit) - VmHWM(ready)` as a corroborating
//! lower-bound diagnostic only (never the primary attribution — a
//! monotonic high-water-mark delta censors to 0 whenever the startup peak
//! exceeds the query-time peak).

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use pulsus_clickhouse::{ChClient, QuerySettings, Row};
use pulsus_read::logql::escape::ch_string;

use super::dataset::{self, BroadDatasetSummary};
use super::paths::{Tables, build_plan, fetch_rows, run_variant_once};
use super::report::{Dist, Variant};
use crate::bench::queries::month_literals;

pub const RSS_REPS: usize = 3;
const RSS_POLL: Duration = Duration::from_millis(10);

// --- RSS probe: parent side ---

/// Reads `VmRSS` (current resident set size) from `/proc/<pid>/status`, in
/// KiB. `None` if the process has already exited or the file cannot be
/// parsed.
fn read_vmrss_kib(pid: u32) -> Option<u64> {
    read_proc_status_field(pid, "VmRSS:")
}

fn read_vmhwm_kib(pid: u32) -> Option<u64> {
    read_proc_status_field(pid, "VmHWM:")
}

fn read_proc_status_field(pid: u32, prefix: &str) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix(prefix) {
            let digits: String = rest.chars().filter(|c| c.is_ascii_digit()).collect();
            return digits.parse().ok();
        }
    }
    None
}

/// Spawns one fresh RSS-probe child (`--rss-probe --rss-variant <V>
/// --rss-breadth <N>`), executes the `READY`/go-signal/`DONE` handshake,
/// polls the parent-side `VmRSS` window every 10 ms, and reaps the child.
/// Returns `(rss_at_ready_kib, rss_peak_kib, child_reported_delta_kib)`.
fn run_one_rss_probe(
    exe: &std::path::Path,
    http_url: &str,
    database: &str,
    user: &str,
    password: &str,
    variant: Variant,
    breadth: u32,
) -> anyhow::Result<(u64, u64, u64)> {
    let mut child: Child = Command::new(exe)
        .args([
            "bench",
            "logs-hydration",
            "--rss-probe",
            "--rss-variant",
            variant.name(),
            "--rss-breadth",
            &breadth.to_string(),
            "--http-url",
            http_url,
            "--database",
            database,
            "--user",
            user,
            "--password",
            password,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;
    let pid = child.id();
    let mut stdin = child.stdin.take().expect("piped stdin");
    let stdout = child.stdout.take().expect("piped stdout");
    let mut reader = BufReader::new(stdout);

    let mut line = String::new();
    reader.read_line(&mut line)?;
    anyhow::ensure!(
        line.trim() == "READY",
        "rss-probe child (variant={}, breadth={breadth}) did not report READY as its first \
         line (got {line:?})",
        variant.name()
    );

    let rss_at_ready = read_vmrss_kib(pid).unwrap_or(0);

    // Go-signal.
    writeln!(stdin, "GO")?;
    stdin.flush()?;
    drop(stdin);

    let mut rss_peak = rss_at_ready;
    let mut done_line = String::new();
    loop {
        // Non-blocking-ish: we can't select on both the pipe and a timer
        // without extra plumbing, so poll: sleep, sample, then attempt a
        // non-blocking-style read via `try_wait` on the child to see if it
        // has already finished (the child also writes DONE before exit —
        // read_line below will unblock once it does).
        std::thread::sleep(RSS_POLL);
        if let Some(v) = read_vmrss_kib(pid) {
            rss_peak = rss_peak.max(v);
        }
        if let Ok(Some(_status)) = child.try_wait() {
            break;
        }
    }
    // Drain the DONE line (best-effort — the child may already have
    // exited by the time we get here).
    let _ = reader.read_line(&mut done_line);

    let status = child.wait()?;
    anyhow::ensure!(
        status.success(),
        "rss-probe child (variant={}, breadth={breadth}) exited non-zero: {status:?}",
        variant.name()
    );

    // The child's own diagnostic HWM delta is parsed from its DONE line
    // (`DONE <ready_hwm> <exit_hwm>`, both self-read from inside the child)
    // — reading `/proc/<pid>/status` from the parent after `wait()` would
    // race an already-reaped process.
    let parts: Vec<&str> = done_line.split_whitespace().collect();
    let child_hwm_delta = if parts.len() == 3 && parts[0] == "DONE" {
        let ready_hwm: u64 = parts[1].parse().unwrap_or(0);
        let exit_hwm: u64 = parts[2].parse().unwrap_or(ready_hwm);
        exit_hwm.saturating_sub(ready_hwm)
    } else {
        0
    };

    Ok((rss_at_ready, rss_peak, child_hwm_delta))
}

/// Runs [`RSS_REPS`] fresh-child RSS probes for `variant`×`breadth`,
/// returning `(client_rss_delta_kib, client_rss_child_hwm_delta_kib)`. The
/// sane-band `rss_suspect` flag is decided by the caller (`paths::
/// run_breadth`), which alone has the decoded envelope's
/// `expected_payload_bytes` (architect plan [R6]).
pub fn run_rss_probe_parent(
    exe: &std::path::Path,
    http_url: &str,
    database: &str,
    user: &str,
    password: &str,
    variant: Variant,
    breadth: u32,
) -> anyhow::Result<(Dist, Dist)> {
    let mut deltas = Vec::with_capacity(RSS_REPS);
    let mut child_hwm_deltas = Vec::with_capacity(RSS_REPS);
    for _ in 0..RSS_REPS {
        let (rss_at_ready, rss_peak, child_hwm_delta) =
            run_one_rss_probe(exe, http_url, database, user, password, variant, breadth)?;
        deltas.push(rss_peak.saturating_sub(rss_at_ready) as f64);
        child_hwm_deltas.push(child_hwm_delta as f64);
    }
    Ok((
        Dist::from_values(deltas),
        Dist::from_values(child_hwm_deltas),
    ))
}

// --- RSS probe: child side ---

/// The hidden `--rss-probe` child mode entry point (architect plan v5
/// [R1]): connects to the already-loaded database, confirms the table is
/// ready, signals `READY`, blocks for the parent's go-signal, runs exactly
/// one `run_variant_once` call, signals `DONE` with its own diagnostic
/// `VmHWM` delta, then returns (the caller exits the process).
pub async fn run_rss_probe_child(
    client: &ChClient,
    tables: &Tables,
    db: &str,
    variant: Variant,
    breadth: u32,
) -> anyhow::Result<()> {
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct CountRow {
        n: u64,
    }
    let count_sql = format!(
        "SELECT count() AS n FROM {} WHERE service = {}",
        tables.streams,
        ch_string(dataset::HYDRATION_SERVICE)
    );
    let n = fetch_rows::<CountRow>(client, &count_sql, &QuerySettings::new())
        .await?
        .into_iter()
        .next()
        .map(|r| r.n)
        .unwrap_or(0);
    anyhow::ensure!(
        n == breadth as u64,
        "rss-probe child: expected {breadth} streams for service {:?} to already be loaded, \
         found {n} — the parent must load the corpus before spawning RSS-probe children",
        dataset::HYDRATION_SERVICE
    );

    // The RSS-probe child does not carry the parent's frozen window bounds
    // (it only knows `--rss-variant`/`--rss-breadth`); it re-derives
    // `end_ns` from the already-loaded corpus's own `log_streams.updated_ns`
    // (every row shares the exact `end_ns` `load_broad_tier` anchored the
    // corpus to) and `start_ns` from the shared `HYDRATION_WINDOW_NS`
    // constant, before signalling READY — this read does not count toward
    // the timed query window, it is setup.
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct EndNsRow {
        end_ns: i64,
    }
    let end_ns_sql = format!(
        "SELECT max(updated_ns) AS end_ns FROM {} WHERE service = {}",
        tables.streams,
        ch_string(dataset::HYDRATION_SERVICE)
    );
    let end_ns = match fetch_rows::<EndNsRow>(client, &end_ns_sql, &QuerySettings::new())
        .await?
        .into_iter()
        .next()
    {
        Some(row) => row.end_ns,
        None => anyhow::bail!("rss-probe child: no log_streams rows found to derive end_ns from"),
    };
    let start_ns = end_ns - dataset::HYDRATION_WINDOW_NS;

    let ready_hwm = read_vmhwm_kib(std::process::id()).unwrap_or(0);
    println!("READY");
    std::io::stdout().flush()?;

    // Block on the parent's go-signal.
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)?;
    anyhow::ensure!(
        line.trim() == "GO",
        "rss-probe child: expected a \"GO\" go-signal on stdin, got {line:?}"
    );

    let sp = build_plan(
        tables,
        db,
        &BroadDatasetSummary {
            breadth,
            service: dataset::HYDRATION_SERVICE.to_string(),
            result_streams: dataset::HYDRATION_RESULT_STREAMS.min(breadth),
            filler_streams: breadth.saturating_sub(dataset::HYDRATION_RESULT_STREAMS),
            result_fingerprints: Vec::new(),
            start_ns,
            end_ns,
            t_split_ns: start_ns + dataset::HYDRATION_WINDOW_NS / 2,
            load_elapsed_ms: 0,
        },
    )?;
    let months = month_literals(sp.start_ns, sp.end_ns);
    let base_id = format!("bench-hydration-rssprobe-{}-{}", variant.name(), breadth);
    run_variant_once(client, tables, &sp, &months, breadth, variant, &base_id).await?;

    let exit_hwm = read_vmhwm_kib(std::process::id()).unwrap_or(ready_hwm);
    println!("DONE {ready_hwm} {exit_hwm}");
    std::io::stdout().flush()?;
    Ok(())
}
