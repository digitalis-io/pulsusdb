#!/usr/bin/env bash
# Issue #87: structured test output + per-suite $GITHUB_STEP_SUMMARY tables
# for the CI API-suite steps (.github/workflows/ci.yml).
#
# Subcommands:
#
#   run <suite-id> <nextest args...>
#       Runs `cargo nextest run --config-file ci/nextest.toml --profile ci
#       <nextest args...>` for one suite, records its JUnit report (copied
#       to <target>/test-summary/junit/<suite-id>.xml for artifact upload)
#       plus a results row, and exits with nextest's own exit code — a
#       failing test fails the step exactly as `cargo test` did. All
#       bookkeeping is best-effort: it can never change the exit code.
#
#   render
#       Appends the per-suite results table (suite, passed, failed,
#       skipped, duration) — plus, when the api_conformance suite ran, its
#       matrix dimensions — to $GITHUB_STEP_SUMMARY (stdout when unset).
#       ALWAYS exits 0: summary generation failures are warnings, never
#       job failures.
#
# State accumulates under <target>/test-summary/ across the job's steps;
# `render` runs once at the end of the job with `if: always()`.

set -uo pipefail

target_dir="${CARGO_TARGET_DIR:-target}"
out_dir="$target_dir/test-summary"
junit_src="$target_dir/nextest/ci/junit.xml"
manifest="crates/pulsus-server/tests/support/manifest.rs"

warn() { echo "::warning::test-summary: $*" >&2; }

# Swatinem/rust-cache restores target/, which may carry a previous run's
# accumulated rows/reports; key the accumulator to this run+job+attempt
# and wipe it on mismatch so stale state never leaks into a summary or an
# uploaded artifact. Called by BOTH `run` and `render` (review finding on
# #87: a job whose steps all fail before the first `run` must render a
# "nothing ran" note, never a cache-restored table).
fresh_dir() {
  local stamp
  stamp="${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}-${GITHUB_JOB:-job}"
  if [ "$(cat "$out_dir/stamp" 2>/dev/null)" != "$stamp" ]; then
    # Explicit && chain (round-2 review finding on #87): a failed
    # rm/mkdir must reach the caller's failure path — a trailing
    # unconditional printf would otherwise mask it with exit 0.
    rm -rf "$out_dir" &&
      mkdir -p "$out_dir/junit" &&
      printf '%s' "$stamp" >"$out_dir/stamp"
  fi
}

# root_attr <name> <junit-file>: attribute off the root <testsuites ...>
# element (nextest writes tests/failures/errors/time there). Empty output
# when absent — callers validate before use and substitute "?".
root_attr() {
  grep -m1 -o '<testsuites [^>]*>' "$2" 2>/dev/null |
    grep -o "$1=\"[^\"]*\"" | head -n1 | cut -d'"' -f2
}

# Strict base-10 count — anything else (empty, "1.0", ".", stray text)
# takes the defensive "?" path instead of reaching arithmetic (review
# finding on #87: a malformed attribute must never be able to change a
# green suite's exit code).
is_count() { [[ "${1:-}" =~ ^[0-9]+$ ]]; }

cmd_run() {
  if [ "$#" -lt 2 ]; then
    echo "usage: ci/test-summary.sh run <suite-id> <nextest args...>" >&2
    exit 2
  fi
  local suite="$1"
  shift
  local slug
  slug=$(printf '%s' "$suite" | tr -c 'A-Za-z0-9_.-' '_')

  fresh_dir || warn "could not reset $out_dir (continuing)"
  # A cache-restored target/ can also carry a stale junit.xml; remove it so
  # a run that dies before writing one is never summarized from old data.
  rm -f "$junit_src" 2>/dev/null || true

  local console="$out_dir/console-$slug.log"
  cargo nextest run --config-file ci/nextest.toml --profile ci "$@" 2>&1 |
    tee "$console"
  local status="${PIPESTATUS[0]}"

  # ---- best-effort bookkeeping; $status is returned untouched ----------
  local tests failures errors time skipped passed result
  tests=$(root_attr tests "$junit_src")
  failures=$(root_attr failures "$junit_src")
  errors=$(root_attr errors "$junit_src")
  time=$(root_attr time "$junit_src")
  if [ -f "$junit_src" ]; then
    cp -f "$junit_src" "$out_dir/junit/$slug.xml" 2>/dev/null ||
      warn "could not copy the JUnit report for $suite"
  else
    warn "no JUnit report produced for $suite"
  fi
  # Ignored tests never appear in nextest's JUnit output (same set skipped
  # as under `cargo test`); take the skipped count from the console
  # summary line instead, ANSI-stripped (CARGO_TERM_COLOR=always).
  skipped=$(sed 's/\x1b\[[0-9;]*m//g' "$console" 2>/dev/null |
    grep -Eo '[0-9]+ skipped' | tail -n1 | grep -Eo '[0-9]+')
  [ -n "$skipped" ] || skipped="?"
  if is_count "$tests" && is_count "$failures" && is_count "$errors"; then
    # Explicit base 10: a leading zero must not trip octal parsing.
    passed=$((10#$tests - 10#$failures - 10#$errors))
    failures=$((10#$failures + 10#$errors))
  else
    if [ -f "$junit_src" ]; then
      warn "unparseable JUnit counts for $suite" \
        "(tests='$tests' failures='$failures' errors='$errors')"
    fi
    passed="?"
    failures="?"
  fi
  [[ "${time:-}" =~ ^[0-9]+(\.[0-9]+)?$ ]] || time="?"
  if [ "$status" -eq 0 ]; then result="pass"; else result="FAIL"; fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$suite" "$passed" "$failures" "$skipped" "${time:-?}" "$result" \
    >>"$out_dir/rows.tsv" || warn "could not record a summary row for $suite"

  exit "$status"
}

cmd_render() {
  local summary="${GITHUB_STEP_SUMMARY:-/dev/stdout}"
  # Same stamp guard as `run`: discards any cache-restored results (and
  # their JUnit files, so the artifact upload can never carry stale data)
  # when no suite step of THIS run recorded anything. If the reset itself
  # fails, whatever is on disk is known-stale — refuse to render it.
  local no_results=""
  if ! fresh_dir; then
    warn "could not reset $out_dir; refusing to render possibly-stale results"
    no_results=1
  elif [ ! -f "$out_dir/rows.tsv" ]; then
    warn "no suite results recorded by this run"
    no_results=1
  fi
  if [ -n "$no_results" ]; then
    {
      echo "### Test results"
      echo
      echo "_No suite results were recorded by this run — an earlier step" \
        "likely failed before any test suite ran (cache-restored results" \
        "are discarded, never rendered)._"
    } >>"$summary" || warn "could not write the step summary"
    return 0
  fi
  {
    echo "### Test results"
    echo
    echo "| Suite | Passed | Failed | Skipped | Duration (s) | Result |"
    echo "|---|---:|---:|---:|---:|---|"
    while IFS=$'\t' read -r suite passed failed skipped dur result; do
      echo "| \`$suite\` | $passed | $failed | $skipped | $dur | $result |"
    done <"$out_dir/rows.tsv"

    # api_conformance coverage at a glance: spawns come from its own JUnit
    # report (one spawned `pulsusdb` permutation per test); mounted-route
    # and case-class-cell counts come from the shared route manifest the
    # suite iterates by construction (tests/support/manifest.rs).
    local api_junit="$out_dir/junit/api_conformance.xml"
    if [ -f "$api_junit" ]; then
      local spawns routes cases
      spawns=$(grep -c '<testcase ' "$api_junit" 2>/dev/null)
      routes=$(grep -c 'status: RouteStatus::Mounted' "$manifest" 2>/dev/null)
      # One cell per `CaseClass {` literal (rustfmt inlines the first
      # element of single-element arrays, so anchor on the literal, not
      # indentation), excluding the struct definition itself.
      cases=$(grep -E 'CaseClass \{' "$manifest" 2>/dev/null |
        grep -vc 'struct CaseClass')
      echo
      echo "**api_conformance matrix:** ${spawns:-?} spawns × ${routes:-?}" \
        "mounted routes; ${cases:-?} negative case-class cells across the" \
        "route families (shared route manifest)"
    fi
  } >>"$summary" || warn "could not write the step summary"
  return 0
}

case "${1:-}" in
run)
  shift
  cmd_run "$@"
  ;;
render)
  cmd_render
  ;;
*)
  echo "usage: ci/test-summary.sh {run <suite-id> <nextest args...> | render}" >&2
  exit 2
  ;;
esac
