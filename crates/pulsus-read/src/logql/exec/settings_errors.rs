use super::*;

/// ClickHouse server exception code for `TOO_MANY_BYTES` — the
/// `max_bytes_to_read` overflow this module sets from
/// `reader.logql_scan_budget_bytes`. One of exactly **two** server codes
/// [`map_read_error`] maps to [`ReadError::QueryTooBroad`] (issue #398
/// added the second, [`CODE_MEMORY_LIMIT_EXCEEDED`]; before it, this was
/// the only one). `max_rows_to_read` is never set on **LogQL** read paths
/// (the traces scan budget sets it deliberately on its generator queries,
/// where code 158 maps to `TooBroadReason::TraceScanBudgetRows` via
/// `traces::exec`'s own mapper — issue #57), so on the LogQL path code
/// 158 (`TOO_MANY_ROWS`) can never masquerade as the byte budget
/// (architect plan amendment §4).
const CODE_TOO_MANY_BYTES: i32 = 307;

/// ClickHouse server exception code for `MEMORY_LIMIT_EXCEEDED` — the
/// `max_memory_usage` overflow issue #398 sets on **every** LogQL read
/// from `reader.logql_read_max_memory_bytes`. Before #398 no LogQL read
/// set a memory limit at all, so this code fell through to
/// [`ReadError::Clickhouse`] and the client saw a `500` carrying the raw
/// server exception — the same "we could not afford this query" condition
/// the byte budget already reported as `422`.
pub(super) const CODE_MEMORY_LIMIT_EXCEEDED: i32 = 241;

/// Doubles every literal `?` in `sql` before execution.
///
/// **Not part of the injection boundary** — this is a `clickhouse` crate
/// quirk, not a SQL-correctness concern: its `SqlBuilder` (`clickhouse`
/// 0.15's `sql::mod::SqlBuilder::new`) treats a bare `?` anywhere in the
/// query text as an unbound bind-argument placeholder (sqlx-style) and
/// fails the query with "unbound query argument" unless doubled (`??`
/// collapses back to one literal `?` before the text reaches the server).
/// This module's SQL is always fully rendered text with no bind
/// arguments, so every `?` is literal — most commonly from a LogQL regex's
/// own `(?:...)` non-capturing-group syntax (`escape::ch_regex_anchored`'s
/// `^(?:...)$` template always contains one), but also from any raw
/// matcher/line-filter value that happens to contain a literal `?`.
/// Applied only at the execution boundary ([`LogQlEngine::query_stream`]):
/// the canonical SQL text `plan`/`sql` generate — and what `PlanExplain`
/// surfaces to callers — is unaffected, so `tests/sql_snapshots.rs`'s
/// byte-exact assertions stay meaningful.
///
/// `pub(crate)`: issue #31's `metrics::exec::MetricsEngine` and issue
/// #57's `traces::exec::TraceEngine` reuse this same fix at their own
/// execution boundaries (their anchored `match(...)` regex predicates
/// carry the identical `^(?:...)$` literal-`?` shape), rather than
/// duplicating the doubling logic.
pub(crate) fn escape_query_placeholders(sql: &str) -> Cow<'_, str> {
    if sql.contains('?') {
        Cow::Owned(sql.replace('?', "??"))
    } else {
        Cow::Borrowed(sql)
    }
}

/// The read-path settings every LogQL query now carries (issue #35): the
/// byte scan budget (`max_bytes_to_read` + `read_overflow_mode = 'throw'`,
/// unchanged from before this issue) plus `max_query_size` — ClickHouse's
/// own SQL-text parse-buffer setting, raised to
/// [`crate::querytext::MAX_QUERY_TEXT_BYTES`] so the documented worst-case
/// stage2/stage3 `IN` lists (at `DEFAULT_MAX_STREAMS`) never trip the
/// 262,144-byte server default. Single source of truth — [`LogQlEngine::
/// budget_settings`]/[`LogQlEngine::paging_settings`] both delegate to this
/// rather than re-deriving the trio, and the `xtask` bench sources
/// [`crate::querytext::MAX_QUERY_TEXT_BYTES`] directly (not this function)
/// to keep its own settings key-for-key identical to what produced the
/// frozen evidence JSONs (issue #35 plan v2, "Frozen-bench resolution").
///
/// Issue #398 adds the per-query memory ceiling —
/// `max_memory_usage = read_max_memory_bytes` (from
/// `reader.logql_read_max_memory_bytes`) plus
/// `max_bytes_before_external_group_by = 0`, i.e. **throw, not spill**: a
/// spilled aggregation is the silently-slow outcome this repo rejects, and
/// the traces precedent (the previous search engine's generator settings,
/// issue #57) set exactly the same pair. Because every LogQL settings object is
/// built here, one edit reaches all eighteen dispatch sites including the
/// shared stage-1 stream resolution several endpoints run — a breach is
/// server code 241 → [`TooBroadReason::LogqlReadMemory`] → `422`, never
/// the raw-exception `500` it was before.
///
/// #603: logs rows sit on the shard that took the push, so these reads send
/// the answer pins with pruning off.
pub fn read_query_settings(
    scan_budget_bytes: u64,
    read_max_memory_bytes: u64,
    skip_unavailable_shards: bool,
) -> QuerySettings {
    QuerySettings::new()
        .answer_pins(false, skip_unavailable_shards)
        .set("max_bytes_to_read", scan_budget_bytes)
        .set("read_overflow_mode", "throw")
        .set("max_query_size", crate::querytext::MAX_QUERY_TEXT_BYTES)
        .set("max_memory_usage", read_max_memory_bytes)
        .set("max_bytes_before_external_group_by", 0u64)
}

/// Pure paging-termination decision (issue #133, the #96
/// `probe_fanout_bound` extraction shape): `true` once the cumulative
/// per-page `read_bytes` has consumed the whole
/// `reader.logql_scan_budget_bytes` budget — the fetch-until-limit loop
/// must return its survivors as partial rather than issue another page
/// (a zero remaining cap would be ClickHouse's *unlimited* sentinel).
/// Extracted from [`LogQlEngine::run_streams_paged`]'s top-of-loop guard
/// so the termination is provable at the max config-accepted budget
/// (`pulsus_config::LOGQL_SCAN_BUDGET_BYTES_CEILING`) with synthetic
/// byte counts. Behavior-identical to the inline `spent >= budget`.
#[inline]
pub(super) fn scan_budget_spent(spent: u64, budget: u64) -> bool {
    spent >= budget
}

/// Maps a ClickHouse error to [`ReadError`], translating the byte-budget
/// overflow code and (issue #398) the memory-ceiling overflow code to
/// structured [`ReadError::QueryTooBroad`] reasons, and leaving every
/// other server code (including 158 `TOO_MANY_ROWS`, which the LogQL path
/// never triggers because it never sets `max_rows_to_read` — the traces
/// search path sets that budget deliberately and maps 158 in its **own**
/// mapper, `traces::exec::map_trace_read_error`, issue #57) as a generic
/// [`ReadError::Clickhouse`] passthrough — never reinterpreted as a
/// timeout or vice versa.
///
/// **The #412 rule.** `ChError::Server.code` used to be parsed out of the
/// exception TEXT on the streaming path, and tenant bytes reach that text, so
/// the code was spoofable. Issue #412 closed that on any server that declares
/// `X-ClickHouse-Exception-Tag` — 26.3, our floor — by never searching result
/// bytes there and slicing the exception out of its tagged frame by declared
/// length (`vendor/clickhouse/PATCHES.md` §2). The search survives only for an
/// **untagged** server (pre-25.11, or a header-stripping proxy), where it is
/// the only signal there is; that arm is documented as forgeable.
///
/// This mapper needed no edit for that, and the reasoning below is why — it is
/// designed so a wrong code can never make anything worse:
///
/// 1. The BOUND is not the parse — `max_memory_usage` is enforced by
///    ClickHouse whether or not we read the code correctly, so a spoofed
///    code only changes the LABEL on a query that was stopped regardless.
/// 2. Fail-open: a MISSED 241 falls through the unchanged
///    [`ReadError::Clickhouse`] arm to the pre-#398 `500`.
/// 3. A FALSE 241 relabels an already-failing query `422`; no data is
///    served and no body content is exposed that the `500` did not.
/// 4. The code drives nothing but status and message — read dispatches
///    never retry (`ChClient::query_stream` has no retry loop; only
///    `execute` retries, and only for idempotent DDL), so a spoofed
///    *retryable* code cannot re-execute a memory-exhausting read.
/// 5. This function is a pure function of the already-parsed `code` field
///    and never re-inspects `message`, so it inherited #412's fix with no
///    edit here.
pub(super) fn map_read_error(
    e: ChError,
    budget_bytes: u64,
    read_max_memory_bytes: u64,
) -> ReadError {
    if let ChError::Server { code, .. } = &e {
        if *code == CODE_TOO_MANY_BYTES {
            return ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes {
                budget_bytes,
                estimate: None,
            });
        }
        if *code == CODE_MEMORY_LIMIT_EXCEEDED {
            return ReadError::QueryTooBroad(TooBroadReason::LogqlReadMemory {
                budget_bytes: read_max_memory_bytes,
            });
        }
    }
    ReadError::Clickhouse(e)
}

/// The Rust-side structural stream cap (task-manager resolution #1 on
/// issue #11): a `count` past `cap` is [`TooBroadReason::StreamCap`], a
/// distinct "too broad" family from the ClickHouse byte budget — never a
/// ClickHouse row limit, since `max_rows_to_read` is never set on LogQL
/// read paths (the traces scan budget sets it deliberately on its
/// generator queries — issue #57); on the LogQL path code 158 cannot
/// masquerade as `StreamCap`.
pub(super) fn check_stream_cap(count: usize, cap: usize) -> Result<(), ReadError> {
    if count > cap {
        Err(ReadError::QueryTooBroad(TooBroadReason::StreamCap {
            count,
            cap,
        }))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Issue #398: a distinctive, non-default `reader.logql_read_max_memory_bytes`
    /// for the settings/mapper tests — deliberately unequal to any scan
    /// budget any test passes, so a mix-up between the two budgets shows up
    /// as a value mismatch rather than an accidental pass.
    const TEST_READ_MEM: u64 = 7_654_321;

    #[test]
    fn code_307_maps_to_scan_budget_bytes() {
        let e = ChError::Server {
            code: 307,
            message: "Code: 307. DB::Exception: Limit for bytes to read exceeded".to_string(),
        };
        let err = map_read_error(e, 1024, TEST_READ_MEM);
        match err {
            ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes { budget_bytes, .. }) => {
                assert_eq!(budget_bytes, 1024);
            }
            other => panic!("expected QueryTooBroad(ScanBudgetBytes), got {other:?}"),
        }
    }

    /// Issue #382. `code_307_maps_to_scan_budget_bytes` above hands the
    /// mapper a `ChError::Server` it built itself, so it cannot see how the
    /// code is READ off the wire — and that is where the defect was. This
    /// starts from the `BadResponse` the `clickhouse` crate hands us when the
    /// server raised 307 AFTER it had already written output: a verbatim
    /// capture from 26.3.17.110 (`SELECT toString(number) AS v FROM
    /// numbers(100000000)` with `max_bytes_to_read=1100000`), whose
    /// `RowBinaryWithNamesAndTypes` column header precedes the exception.
    ///
    /// The `Code: N` prefix is the one the ADR 0007 vendored patch
    /// (`vendor/clickhouse/PATCHES.md`) takes from
    /// `X-ClickHouse-Exception-Code` and puts at byte 0. Without it the result
    /// bytes cannot be parsed for a code soundly, so the client gets 500
    /// `internal` instead of the 422 `query_too_broad` that tells them to
    /// narrow the query.
    #[test]
    fn a_307_raised_after_output_was_written_still_maps_to_scan_budget_bytes() {
        let body = "\u{1}\u{1}v\u{6}StringCode: 307. DB::Exception: Limit for rows or bytes to \
                    read exceeded, max bytes: 1.05 MiB, current bytes: 1.50 MiB: While executing \
                    NumbersRange. (TOO_MANY_BYTES) (version 26.3.17.110 (official build))";
        assert_eq!(body.len(), 209, "verbatim capture, pinned against an edit");
        let raw = ChError::from(clickhouse::error::Error::BadResponse(body.to_string()));
        assert!(matches!(
            map_read_error(raw, 1024, TEST_READ_MEM),
            ReadError::Clickhouse(_)
        ));

        let patched = ChError::from(clickhouse::error::Error::BadResponse(format!(
            "Code: 307\n{body}"
        )));
        match map_read_error(patched, 1024, TEST_READ_MEM) {
            ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes { budget_bytes, .. }) => {
                assert_eq!(budget_bytes, 1024);
            }
            other => panic!("expected QueryTooBroad(ScanBudgetBytes), got {other:?}"),
        }
    }

    #[test]
    fn code_158_is_not_mapped_to_query_too_broad() {
        let e = ChError::Server {
            code: 158,
            message: "Code: 158. DB::Exception: Limit for rows to read exceeded".to_string(),
        };
        let err = map_read_error(e, 1024, TEST_READ_MEM);
        assert!(matches!(err, ReadError::Clickhouse(_)));
    }

    /// Issue #398 AC L3: server code 241 becomes the named LogQL read
    /// memory refusal (`422`), carrying the CONFIGURED ceiling — not the
    /// scan budget, which is a different number for a different resource.
    /// 307 and 158 keep their pre-#398 outcomes, so the new arm cannot
    /// have widened.
    #[test]
    fn map_read_error_maps_code_241_to_the_logql_read_memory_reason() {
        let e = ChError::Server {
            code: 241,
            message: "Code: 241. DB::Exception: Memory limit (for query) exceeded: would use \
                      5.02 MiB"
                .to_string(),
        };
        match map_read_error(e, 1024, TEST_READ_MEM) {
            ReadError::QueryTooBroad(TooBroadReason::LogqlReadMemory { budget_bytes }) => {
                assert_eq!(budget_bytes, TEST_READ_MEM);
                assert_ne!(
                    budget_bytes, 1024,
                    "the memory ceiling must never be reported as the byte scan budget"
                );
            }
            other => panic!("expected QueryTooBroad(LogqlReadMemory), got {other:?}"),
        }
        // 307 still the byte budget.
        let e307 = ChError::Server {
            code: 307,
            message: "Limit for bytes to read exceeded".to_string(),
        };
        assert!(matches!(
            map_read_error(e307, 1024, TEST_READ_MEM),
            ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes {
                budget_bytes: 1024,
                ..
            })
        ));
        // 158 still unmapped: the LogQL path sets no row cap.
        let e158 = ChError::Server {
            code: 158,
            message: "Limit for rows to read exceeded".to_string(),
        };
        assert!(matches!(
            map_read_error(e158, 1024, TEST_READ_MEM),
            ReadError::Clickhouse(_)
        ));
    }

    /// Issue #398 AC L6 (the #412 rule): the classification is a pure
    /// function of the already-parsed `code` field and NEVER re-inspects
    /// `message`. Two directions:
    ///
    /// - a genuine 241 whose message contains no `Code:` prefix anywhere
    ///   still classifies, so the mapping does not secretly depend on the
    ///   text; and
    /// - a code-153 failure whose message embeds a user regex containing a
    ///   forged `Code: 241` — the exact #412 spoof shape, measured live on
    ///   24.8.14.39 — does NOT become a memory refusal.
    ///
    /// Together these pinned that when #412 replaced the exception-code parse
    /// the classification here would become sound with no edit to this file.
    /// It did, and there was none.
    #[test]
    fn logql_read_memory_classification_reads_only_the_server_code() {
        let bare = ChError::Server {
            code: 241,
            message: "Memory limit (for query) exceeded".to_string(),
        };
        assert!(matches!(
            map_read_error(bare, 1024, TEST_READ_MEM),
            ReadError::QueryTooBroad(TooBroadReason::LogqlReadMemory { .. })
        ));

        let spoofed = ChError::Server {
            code: 153,
            message: "Division by zero: while executing 'FUNCTION and(match(toString(number), \
                      'Code: 241. DB::Exception: forged|.*'_String))'"
                .to_string(),
        };
        assert!(matches!(
            map_read_error(spoofed, 1024, TEST_READ_MEM),
            ReadError::Clickhouse(_)
        ));
    }

    /// Issue #398 AC L7 (the #412 fail-open rule): a code that is NOT one
    /// of the two this mapper claims falls through to
    /// `ReadError::Clickhouse` — i.e. to the pre-#398 outcome (500, or 504
    /// for a timeout). A MISSED 241 therefore costs nothing that was not
    /// already being paid, which is what makes classifying on a spoofable
    /// code safe.
    ///
    /// 210 is in `pulsus-clickhouse`'s `RETRYABLE_SERVER_CODES` and is
    /// swept here deliberately: read dispatches never retry
    /// (`ChClient::query_stream` has no retry loop; only `execute` retries,
    /// and only for idempotent DDL), so a spoofed retryable code cannot
    /// re-execute a memory-exhausting read.
    #[test]
    fn a_misclassified_server_code_falls_back_to_the_pre_398_outcome() {
        for code in [0, 158, 191, 210, 396, 999] {
            let e = ChError::Server {
                code,
                message: format!("server said {code}"),
            };
            assert!(
                matches!(
                    map_read_error(e, 1024, TEST_READ_MEM),
                    ReadError::Clickhouse(_)
                ),
                "code {code} must fall open to the pre-#398 Clickhouse passthrough"
            );
        }
        // The two the mapper DOES claim, asserted in the same sweep so the
        // table cannot silently become vacuous.
        for (code, claimed) in [(241, "LogqlReadMemory"), (307, "ScanBudgetBytes")] {
            let e = ChError::Server {
                code,
                message: format!("server said {code}"),
            };
            assert!(
                matches!(
                    map_read_error(e, 1024, TEST_READ_MEM),
                    ReadError::QueryTooBroad(_)
                ),
                "code {code} must map to {claimed}"
            );
        }
    }

    /// Issue #35: `read_query_settings` — the single source of truth
    /// `budget_settings`/`paging_settings` delegate to — carries exactly
    /// the byte-budget pair plus the raised `max_query_size`.
    #[test]
    fn read_query_settings_sets_the_scan_budget_and_the_raised_query_text_cap() {
        let s = read_query_settings(1024, TEST_READ_MEM, false);
        assert_eq!(s.get("max_bytes_to_read"), Some("1024"));
        assert_eq!(s.get("read_overflow_mode"), Some("throw"));
        assert_eq!(
            s.get("max_query_size"),
            Some(crate::querytext::MAX_QUERY_TEXT_BYTES.to_string().as_str())
        );
        // Issue #757: the body's `text` index is gone, and with it the
        // setting that turned its direct read off.
        assert_eq!(s.get("query_plan_direct_read_from_text_index"), None);
    }

    /// **T37.** The three discovery dispatches carry `budget_settings`' set
    /// and nothing more, clustered or not — **`activity_query_settings` no
    /// longer exists** (issue #603).
    ///
    /// The activity scan is a statement of its own now, so no discovery read
    /// nests a distributed table inside an `IN (subquery)` and
    /// `distributed_product_mode` has nothing to govern on this path. Sending
    /// it anyway would be a pin over a shape that is not generated.
    ///
    /// **This is a completeness claim about a settings constructor that is
    /// gone**, so it is stated as the absence of the name from what every
    /// LogQL read carries: `read_query_settings` is the one constructor left,
    /// and neither it nor the paging form names the setting.
    #[test]
    fn the_discovery_reads_send_no_distributed_product_mode() {
        const MEM: u64 = 4096;
        const BUDGET: u64 = 1_000_000;

        let budget = read_query_settings(BUDGET, MEM, false);
        assert_eq!(
            budget.get("distributed_product_mode"),
            None,
            "the discovery reads dispatch with these settings, and they must \
             carry no product-mode pin: there is no nested distributed \
             subquery left for it to govern"
        );
        let paging = read_query_settings(BUDGET, MEM, false).set("wait_end_of_query", 1);
        assert_eq!(paging.get("distributed_product_mode"), None);

        // What they do carry is unchanged.
        assert_eq!(budget.get("max_bytes_to_read"), Some("1000000"));
        assert_eq!(budget.get("read_overflow_mode"), Some("throw"));
        assert_eq!(budget.get("max_memory_usage"), Some("4096"));

        // And no transfer pin either: the earlier revision of this design
        // pinned `max_rows_to_transfer` and `transfer_overflow_mode` on a
        // `GLOBAL IN`, and measured against the server version the clustered
        // fixture pins, that pair does not bind the statement this path
        // generates. What bounds the set now is `check_stream_cap` and the
        // rendered-SQL ceiling, both already in the tree.
        // `transfer_overflow_mode` and `set_overflow_mode` are sent, as
        // `throw`: they are answer pins (#603 follow-up), and a mode with no
        // limit beside it bounds nothing.
        for absent in [
            "max_rows_to_transfer",
            "max_bytes_to_transfer",
            "max_rows_in_set",
        ] {
            assert_eq!(
                budget.get(absent),
                None,
                "{absent} must not be sent: no discovery read builds a \
                 server-side set any more"
            );
        }
    }

    /// Issue #398 AC L1: every LogQL read carries the per-query memory
    /// ceiling and refuses to SPILL. `max_bytes_before_external_group_by =
    /// 0` is the throw-not-spill half — with spilling enabled a wide
    /// aggregation would grind through disk instead of failing loud, which
    /// is the silently-slow outcome this repo rejects (the traces
    /// precedent, the previous search engine's generator settings, issue
    /// #57, set the identical pair).
    ///
    /// The three pre-existing keys must survive untouched: the whole point
    /// of putting the ceiling in this one function is that it ADDS to the
    /// budget every dispatch site already carried.
    #[test]
    fn read_query_settings_carry_the_memory_ceiling_and_throw_not_spill() {
        let s = read_query_settings(1024, 4096, false);
        assert_eq!(s.get("max_memory_usage"), Some("4096"));
        assert_eq!(s.get("max_bytes_before_external_group_by"), Some("0"));
        // The pre-#398 trio, unchanged.
        assert_eq!(s.get("max_bytes_to_read"), Some("1024"));
        assert_eq!(s.get("read_overflow_mode"), Some("throw"));
        assert_eq!(
            s.get("max_query_size"),
            Some(crate::querytext::MAX_QUERY_TEXT_BYTES.to_string().as_str())
        );
    }

    /// Issue #398 AC L2: EVERY settings constructor on this path carries
    /// the ceiling, and the byte budget never leaks into it.
    ///
    /// The two engine methods (`budget_settings`, `paging_settings`) are
    /// verbatim delegations to the one free function asserted here —
    /// `budget_settings` is `read_query_settings(scan_budget,
    /// read_max_memory, false)` and `paging_settings` is
    /// `read_query_settings(remaining, …)` plus `wait_end_of_query`. They take
    /// `&self` and so need a live `ChClient`; the free function exists
    /// precisely so the settings decision is provable without one. **Every
    /// discovery dispatch now takes `budget_settings` too** (issue #603): the
    /// activity scan is its own statement, so there is no nested distributed
    /// subquery for a product-mode setting to govern.
    /// The engine-level completeness claim — that no dispatch site
    /// bypasses these constructors — is proved live instead, by
    /// `every_logql_engine_query_carries_the_memory_ceiling` in
    /// `tests/query_log_gates.rs`.
    #[test]
    fn every_logql_settings_constructor_carries_the_memory_ceiling() {
        const MEM: u64 = 4096;
        const BUDGET: u64 = 1_000_000;
        // `budget_settings`' body, which every discovery dispatch shares.
        let budget = read_query_settings(BUDGET, MEM, false);
        assert_eq!(
            budget.get("max_memory_usage"),
            Some(MEM.to_string().as_str()),
            "budget_settings must carry the memory ceiling"
        );
        assert_eq!(
            budget.get("max_bytes_before_external_group_by"),
            Some("0"),
            "budget_settings must throw rather than spill"
        );

        // `paging_settings(remaining)`' body: the DECREMENTED budget lands
        // in `max_bytes_to_read` only. The memory ceiling is per query and
        // is never decremented, so a late page carries the same ceiling as
        // the first — if the decrement ever leaked into it, page N would
        // silently run with a smaller memory bound than page 1.
        const REMAINING: u64 = 777;
        let paging = read_query_settings(REMAINING, MEM, false).set("wait_end_of_query", 1);
        assert_eq!(paging.get("max_bytes_to_read"), Some("777"));
        assert_eq!(
            paging.get("max_memory_usage"),
            Some(MEM.to_string().as_str())
        );
        assert_eq!(paging.get("max_bytes_before_external_group_by"), Some("0"));
        assert_eq!(paging.get("wait_end_of_query"), Some("1"));
    }

    /// Issue #133: the read settings carry the byte scan budget VERBATIM
    /// at the accepted minimum (1) and at the maximum config-accepted
    /// `reader.logql_scan_budget_bytes` — never ClickHouse's `0`
    /// (unlimited) sentinel.
    #[test]
    fn read_query_settings_carry_the_budget_verbatim_at_the_accepted_min_and_ceiling() {
        assert_eq!(
            read_query_settings(1, TEST_READ_MEM, false).get("max_bytes_to_read"),
            Some("1")
        );
        let cap = pulsus_config::LOGQL_SCAN_BUDGET_BYTES_CEILING;
        let s = read_query_settings(cap, TEST_READ_MEM, false);
        assert_eq!(
            s.get("max_bytes_to_read"),
            Some(cap.to_string().as_str()),
            "the ceiling budget must pass through verbatim"
        );
        assert_ne!(s.get("max_bytes_to_read"), Some("0"));
    }

    /// U1 (#603 follow-up): every LogQL read carries the answer pins, with
    /// pruning off and the option as configured, in both the budget and the
    /// paging form.
    #[test]
    fn every_logs_read_carries_the_answer_pins() {
        for skip in [false, true] {
            let pins = QuerySettings::new().answer_pins(false, skip);
            let budget = read_query_settings(1024, TEST_READ_MEM, skip);
            let paging = read_query_settings(1024, TEST_READ_MEM, skip).set("wait_end_of_query", 1);
            for (form, s) in [("budget", &budget), ("paging", &paging)] {
                for name in pulsus_clickhouse::ANSWER_PIN_NAMES {
                    assert_eq!(
                        s.get(name),
                        pins.get(name),
                        "{form} settings, skip {skip}: {name}"
                    );
                }
            }
        }
    }

    /// Issue #133: the paging loop's termination guard still fires at the
    /// maximum config-accepted budget — `spent == budget` terminates
    /// (never issues a zero/unlimited remaining cap), one byte under
    /// does not. Synthetic counts; the extracted decision IS the
    /// top-of-loop guard in `run_streams_paged`.
    #[test]
    fn paging_termination_still_fires_at_the_max_accepted_scan_budget() {
        let cap = pulsus_config::LOGQL_SCAN_BUDGET_BYTES_CEILING;
        assert!(scan_budget_spent(cap, cap));
        assert!(!scan_budget_spent(cap - 1, cap));
    }

    /// Issue #35 acceptance criterion 2: the full-shape admission
    /// identity — `stage2`'s worst-case rendering (100,000 `u64::MAX`
    /// fingerprint literals, the documented `DEFAULT_MAX_STREAMS` cap)
    /// fits comfortably under [`crate::querytext::MAX_QUERY_TEXT_BYTES`]
    /// while exceeding ClickHouse's 262,144-byte default — proving the
    /// raised setting is load-bearing, not vacuous.
    #[test]
    fn stage2_at_default_max_streams_worst_case_fits_under_the_query_text_cap() {
        let fps: Vec<FpLiteral> = std::iter::repeat_n(
            Fingerprint::from_raw(u128::MAX).sql_literal(),
            super::super::params::DEFAULT_MAX_STREAMS,
        )
        .collect();
        let sql = super::super::sql::stage2(&longest_tenant(), "log_streams", &fps);
        let bytes = sql.len() as u64;
        assert!(
            bytes > 262_144,
            "worst-case stage2 SQL ({bytes} B) must exceed the ClickHouse default cap to prove \
             the raised setting is load-bearing"
        );
        assert!(
            bytes < crate::querytext::MAX_QUERY_TEXT_BYTES,
            "worst-case stage2 SQL ({bytes} B) must fit under the {}-byte cap",
            crate::querytext::MAX_QUERY_TEXT_BYTES
        );
    }

    /// The longest tenant the header admits, 150 bytes (issue #653): the
    /// worst-case statements below carry it.
    fn longest_tenant() -> pulsus_model::Tenant {
        let value = "a".repeat(pulsus_model::MAX_TENANT_BYTES);
        pulsus_model::Tenant::from_header(
            Some(&http::HeaderValue::from_str(&value).expect("a header value")),
            false,
        )
        .expect("a valid tenant")
    }

    /// The full guaranteed-admitted envelope this issue's plan derives:
    /// 100,000 worst-case fingerprints + 10,000 escaped 64-byte service
    /// literals + 1 MiB of pre-rendered line-filter predicate text ≈ 3.73
    /// MiB — comfortably under the 8 MiB cap, comfortably over the
    /// ClickHouse default.
    fn worst_case_envelope() -> (
        Vec<FpLiteral>,
        Vec<CheckedLiteral>,
        Vec<super::super::predicate::CheckedFragment>,
    ) {
        let fps: Vec<FpLiteral> = std::iter::repeat_n(
            Fingerprint::from_raw(u128::MAX).sql_literal(),
            super::super::params::DEFAULT_MAX_STREAMS,
        )
        .collect();
        // 64-byte literals (`'` + 62 chars + `'`). Issue #286: minted, not
        // hand-written — `predicate::literal` IS `ch_string`, and a
        // 62-digit value escapes to nothing, so the rendered bytes are the
        // same 64 as before.
        let services: Vec<CheckedLiteral> = (0..10_000)
            .map(|i| super::super::predicate::literal(&format!("{i:062}")))
            .collect();
        // 16 × 64 KiB pre-rendered predicates ≈ 1 MiB, a generous multiple
        // of any realistic compiled line-filter chain.
        //
        // Issue #286: a raw 65,536-byte string is no longer constructible
        // as a fragment, so the envelope is MINTED at exactly that size and
        // the size is asserted rather than assumed. `-` is neither `%`, `_`
        // nor `\`, so neither `ch_like_contains` nor `ch_string` escapes
        // anything: a `|=` filter over N dashes renders its exact predicate
        // `body LIKE '%<N dashes>%'`, which is `10 + 2 + N + 2` bytes
        // (issue #450). N = 65,522 gives exactly 65,536. Issue #839 puts the
        // lowered-literal check in front, `(<check> AND <exact>)`, whose
        // literal is the run's first 256 bytes: 293 bytes of check and 7 of
        // joining, 65,836 in all. Sixteen checks are 4,688 bytes, inside the
        // query's 65,536-byte check budget.
        let filter = super::super::predicate::line_filter(&pulsus_logql::LineFilter {
            op: pulsus_logql::LineFilterOp::Contains,
            head: pulsus_logql::LineMatch::Text("-".repeat(65_522)),
            or_matches: Vec::new(),
        })
        .expect("a Contains filter compiles no regex");
        assert_eq!(
            filter.as_sql().len(),
            65_536 + 293 + 7,
            "the envelope's per-filter size is the pre-#286 figure, exactly, with its check"
        );
        let line_filters: Vec<super::super::predicate::CheckedFragment> =
            std::iter::repeat_n(filter, 16).collect();
        (fps, services, line_filters)
    }

    #[test]
    fn stage3_at_the_full_worst_case_envelope_fits_under_the_query_text_cap() {
        let (fps, services, line_filters) = worst_case_envelope();
        let sql = super::super::sql::stage3(
            &longest_tenant(),
            "log_samples",
            &services,
            &fps,
            super::super::sql::TimeWindow {
                start_ns: 0,
                end_ns: i64::MAX,
            },
            &line_filters,
            Direction::Backward,
            u32::MAX,
        );
        let bytes = sql.len() as u64;
        assert!(bytes > 262_144, "stage3 envelope SQL is {bytes} B");
        assert!(
            bytes < crate::querytext::MAX_QUERY_TEXT_BYTES,
            "stage3 envelope SQL is {bytes} B, expected < {}",
            crate::querytext::MAX_QUERY_TEXT_BYTES
        );
    }

    #[test]
    fn stage3_keyset_at_the_full_worst_case_envelope_fits_under_the_query_text_cap() {
        let (fps, services, line_filters) = worst_case_envelope();
        let sql = super::super::sql::stage3_keyset(
            &longest_tenant(),
            "log_samples",
            &services,
            &fps,
            super::super::sql::TimeWindow {
                start_ns: 0,
                end_ns: i64::MAX,
            },
            super::super::sql::KeysetLower::After {
                tuple: (
                    i64::MAX,
                    Fingerprint::from_raw(u128::MAX).sql_literal(),
                    u64::MAX,
                ),
                offset: u32::MAX,
            },
            Direction::Backward,
            &line_filters,
            u32::MAX,
        );
        let bytes = sql.len() as u64;
        assert!(bytes > 262_144, "stage3_keyset envelope SQL is {bytes} B");
        assert!(
            bytes < crate::querytext::MAX_QUERY_TEXT_BYTES,
            "stage3_keyset envelope SQL is {bytes} B, expected < {}",
            crate::querytext::MAX_QUERY_TEXT_BYTES
        );
    }

    #[test]
    fn exceeding_the_stream_cap_maps_to_stream_cap_not_scan_budget_bytes() {
        let err = check_stream_cap(100_001, 100_000).unwrap_err();
        match err {
            ReadError::QueryTooBroad(TooBroadReason::StreamCap { count, cap }) => {
                assert_eq!(count, 100_001);
                assert_eq!(cap, 100_000);
            }
            other => panic!("expected QueryTooBroad(StreamCap), got {other:?}"),
        }
    }

    #[test]
    fn a_count_at_or_below_the_cap_is_not_too_broad() {
        assert!(check_stream_cap(100_000, 100_000).is_ok());
        assert!(check_stream_cap(1, 100_000).is_ok());
    }

    #[test]
    fn a_generic_server_error_passes_through_unmapped() {
        let e = ChError::Server {
            code: 62,
            message: "syntax error".to_string(),
        };
        assert!(matches!(
            map_read_error(e, 1024, TEST_READ_MEM),
            ReadError::Clickhouse(_)
        ));
    }

    #[test]
    fn a_timeout_is_never_reinterpreted_as_a_budget_error() {
        let e = ChError::Timeout("deadline".to_string());
        assert!(matches!(
            map_read_error(e, 1024, TEST_READ_MEM),
            ReadError::Clickhouse(_)
        ));
    }

    #[test]
    fn escape_query_placeholders_doubles_a_literal_question_mark() {
        assert_eq!(
            escape_query_placeholders("match(val, '^(?:prod|staging)$')"),
            "match(val, '^(??:prod|staging)$')"
        );
    }

    #[test]
    fn escape_query_placeholders_doubles_every_occurrence() {
        assert_eq!(escape_query_placeholders("a? b? c?"), "a?? b?? c??");
    }

    #[test]
    fn escape_query_placeholders_leaves_sql_without_question_marks_untouched() {
        let sql = "SELECT fingerprint FROM log_streams_idx WHERE key = 'env'";
        assert_eq!(escape_query_placeholders(sql), sql);
    }

    /// Round-2 review, finding rejected (sound round-trip, verified against
    /// `clickhouse` 0.15.1's `SqlBuilder::new`): each literal `?` maps to
    /// `??`, so a user regex containing a literal `??` (e.g. `a??`) becomes
    /// `a????` here — an even-length run of 4, which the crate's lexer
    /// pairs cleanly back into 2 literal `?`s, restoring the original `a??`
    /// exactly. The full escape→execute→unbind round-trip against a live
    /// server isn't unit-testable here (`SqlBuilder` is `pub(crate)` to
    /// the `clickhouse` crate); it's covered end-to-end by the live
    /// `stage1_regex_matcher_...` / `stage3_regex_line_filter_...` /
    /// `stage3_not_regex_line_filter_...` `EXPLAIN` cases
    /// (`tests/explain_indexes.rs`), whose `(?:...)`/metacharacter regex
    /// patterns execute successfully against ClickHouse.
    #[test]
    fn escape_query_placeholders_doubles_a_literal_double_question_mark() {
        assert_eq!(escape_query_placeholders("a??"), "a????");
        assert_eq!(escape_query_placeholders("????"), "????????");
    }
}
