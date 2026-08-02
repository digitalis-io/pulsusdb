//! Issue #311 — the LogQL template timezone is SERVER CONFIGURATION, not
//! host state.
//!
//! `TemplateEnv::process()` used to resolve the zone from `$TZ`, falling
//! back to `/etc/localtime`, so `line_format`/`label_format` templates
//! that render local times produced different text on different machines
//! answering the same query. The zone now comes from
//! `reader.template_timezone` (default `UTC`), installed once per process,
//! and nothing on the read path reads the host environment.
//!
//! **This is one `#[test]` in a file of its own, deliberately.** The
//! setting is a process-wide `OnceLock` and the ambient leg mutates the
//! process environment; both are only meaningful when nothing else runs
//! concurrently in the same binary, and cargo gives each integration test
//! file its own process. The two legs are ordered inside the one function:
//! the ambient leg must run *before* anything is installed, since it is
//! asserting the DEFAULT is unaffected by the host.
//!
//! **Why the ambient leg is not vacuous.** It sets `$TZ` to a zone whose
//! offset is nowhere near UTC and asserts the rendering is unchanged — an
//! assertion the pre-#311 code failed. The configured leg then renders the
//! very same fixture in a third zone and asserts the output *does* change,
//! so "unchanged" is a property of the source of the zone, not of a
//! template that happens to be zone-insensitive.

use pulsus_read::logql::CompiledPipeline;
use pulsus_read::logql::template::{configured_env, install_template_timezone, template_timezone};

/// A fixed instant with a fixed zone-sensitive rendering: Go's
/// `time.Time.String()` prints the offset and the zone abbreviation, so
/// the same nanosecond reads differently in every zone.
const TS_NS: i64 = 1_700_000_000_000_000_000;

/// `{{ __timestamp__ }}` — the entry timestamp as a `Local` `time.Time`,
/// which is exactly the value the reference resolves from the process.
const QUERY: &str = r#"{app="x"} | line_format "{{ __timestamp__ }}""#;

/// Renders [`QUERY`] at [`TS_NS`] through a freshly compiled pipeline —
/// i.e. through whatever environment `CompiledPipeline::compile` installs,
/// which is the production path (`with_template_env` is deliberately NOT
/// used here: overriding the env would test the override, not the source).
fn render() -> String {
    let expr = pulsus_logql::parse(QUERY).expect("fixture parses");
    let pulsus_logql::Expr::Log(log) = expr else {
        panic!("fixture is a log query");
    };
    let compiled = CompiledPipeline::compile(&log.pipeline).expect("compile");
    let base = vec![("app".to_string(), "x".to_string())];
    let out = compiled
        .run("original line", &base, TS_NS)
        .expect("render is within budget")
        .expect("no stage drops the line");
    out.line.into_owned()
}

#[test]
fn the_template_timezone_comes_from_configuration_and_never_from_the_host() {
    // ---- Leg 1: the host cannot influence the default. ----------------
    //
    // `Pacific/Kiritimati` is UTC+14 — a different calendar DAY from UTC
    // for part of every day — so a host-resolved zone could not possibly
    // coincide with the UTC rendering by accident.
    //
    // SAFETY: this is the only test in this binary (see the module doc),
    // so no other thread is reading the environment concurrently. It is
    // set purely to prove it is ignored — no production path reads `$TZ`.
    unsafe {
        std::env::set_var("TZ", "Pacific/Kiritimati");
    }

    assert_eq!(
        template_timezone(),
        chrono_tz::Tz::UTC,
        "the documented default is UTC, whatever the host says"
    );
    let default_rendered = render();
    assert_eq!(
        default_rendered, "2023-11-14 22:13:20 +0000 UTC",
        "an unconfigured server renders in UTC even with $TZ set to UTC+14"
    );
    // Same claim about the environment the compiler installs, so a future
    // refactor that stops threading it through `compile` still reddens.
    let env = configured_env();
    assert!(
        env.local.is_none() && env.local_name.is_none(),
        "the default environment is Go's degenerate UTC Local, got {env:?}"
    );

    // ---- Leg 2: two nodes with the same configuration agree. ---------
    //
    // Compiling twice under one installed zone models two servers running
    // one configuration: the value is read from the same declared source,
    // not discovered per process.
    assert_eq!(
        render(),
        default_rendered,
        "two compiles under one configuration must render identically"
    );

    // ---- Leg 3: configuring a zone reproduces the reference. ----------
    //
    // Also the proof that leg 1 is not vacuous: the same fixture renders
    // differently once the zone is DECLARED, so leg 1's "unchanged" is
    // about where the zone came from, not about the template.
    install_template_timezone(chrono_tz::Tz::Europe__London).expect("first install succeeds");
    assert_eq!(template_timezone(), chrono_tz::Tz::Europe__London);
    let configured_rendered = render();
    assert_eq!(
        configured_rendered, "2023-11-14 22:13:20 +0000 GMT",
        "a configured zone renders in that zone, with its own name"
    );
    assert_ne!(
        configured_rendered, default_rendered,
        "the fixture is zone-sensitive, so leg 1 asserted something"
    );
    // The zone NAME follows the reference's `$TZ=<name>` branch (the
    // loaded zone keeps its own name), not its `/etc/localtime` branch
    // (which renames the result to "Local") — PulsusDB has no such branch.
    let env = configured_env();
    assert_eq!(env.local, Some(chrono_tz::Tz::Europe__London));
    assert_eq!(env.local_name.as_deref(), Some("Europe/London"));

    // ---- Leg 4: the setting is install-once. -------------------------
    let err = install_template_timezone(chrono_tz::Tz::America__New_York)
        .expect_err("a second install must be refused, not silently applied");
    assert_eq!(err.installed, "Europe/London");
    assert_eq!(err.attempted, "America/New_York");
    assert_eq!(
        render(),
        configured_rendered,
        "a refused install must not have changed the effective zone"
    );

    // SAFETY: as above — single-threaded by construction; restores the
    // environment this test found.
    unsafe {
        std::env::remove_var("TZ");
    }
}
