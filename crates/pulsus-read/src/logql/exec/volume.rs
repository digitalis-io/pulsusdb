use super::*;

// ---------------------------------------------------------------------
// Issue #169 (M7-C1): `/api/logs/v1/volume`.
// ---------------------------------------------------------------------

/// `aggregateBy` (docs/api.md §2.6): group volumes by the matched label
/// *pairs* (`series`, the default) or by bare label *names* (`labels`,
/// each entry keyed `(name, "")`). Semantics pinned against the repo's
/// interop oracle, grafana/loki:3.4.2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolumeAggregateBy {
    Series,
    Labels,
}

/// One `/api/logs/v1/volume` request's engine parameters. `target_labels`
/// is already deduped and bounded by the API layer (`logs_api/params.rs`'s
/// `MAX_TARGET_LABELS`/`MAX_TARGET_LABEL_BYTES` caps run BEFORE any AST
/// mutation here); empty = key by the selector's own matcher names.
#[derive(Debug, Clone)]
pub struct VolumeQuery {
    pub bounds: TimeBounds,
    /// Post-aggregation top-N truncation (bytes-desc).
    pub limit: u32,
    pub aggregate_by: VolumeAggregateBy,
    /// Deduped; empty = none.
    pub target_labels: Vec<String>,
}

/// One aggregated volume entry. `labels` sorted by name; empty vec = the
/// `{}` group. In Labels mode: exactly one pair `(label_name, "")`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeEntry {
    pub labels: Vec<(String, String)>,
    pub bytes: u64,
}

impl LogQlEngine {
    /// `/api/logs/v1/volume` (issue #169, docs/api.md §2.6): per-label-set
    /// byte volumes over `[start, end]`, served ENTIRELY from the rollup —
    /// the endpoint accepts a matchers-only selector, so unlike
    /// [`LogQlEngine::stats`] there is no raw fallback and never a body
    /// read. Keying/sort semantics oracle-pinned (grafana/loki:3.4.2):
    /// see [`accumulate_volume`].
    pub async fn volume(
        &self,
        expr: &Expr,
        q: &VolumeQuery,
    ) -> Result<Vec<VolumeEntry>, ReadError> {
        self.volume_inner(expr, q, None).await
    }

    /// [`LogQlEngine::volume`] plus its `X-Pulsus-Explain` trace, in the
    /// same single pass (no second scan) — the `query_explained` contract.
    pub async fn volume_explained(
        &self,
        expr: &Expr,
        q: &VolumeQuery,
    ) -> Result<(Vec<VolumeEntry>, PlanExplain), ReadError> {
        let mut explain = PlanExplain::new("volume");
        let entries = self.volume_inner(expr, q, Some(&mut explain)).await?;
        Ok((entries, explain))
    }

    async fn volume_inner(
        &self,
        expr: &Expr,
        q: &VolumeQuery,
        mut explain: Option<&mut PlanExplain>,
    ) -> Result<Vec<VolumeEntry>, ReadError> {
        // Matchers-only (defense in depth — the API layer rejects any
        // pipeline stage 400 before parsing reaches the engine): the
        // rollup is body-content-blind, so even a line filter would
        // silently over-count here, and volume deliberately has NO
        // raw fallback (docs/api.md §2.6).
        let le = match expr {
            Expr::Log(le) if le.pipeline.is_empty() => le,
            Expr::Log(_) => {
                return Err(ReadError::PipelineInvalid {
                    reason: "volume supports a bare stream selector only (no pipeline stages)"
                        .to_string(),
                });
            }
            Expr::Metric(_) => {
                return Err(ReadError::PipelineInvalid {
                    reason: "volume requires a log stream selector query (a metric query has no \
                             stream volume)"
                        .to_string(),
                });
            }
        };
        let labels_to_match = volume_labels_to_match(&le.selector, &q.target_labels);
        // `targetLabels` injection (oracle `prepareLabelsAndMatchersWithTargets`):
        // each target with no matcher of its name gets a `=~ ".+"` matcher
        // appended BEFORE planning, so target-keyed streams are resolvable
        // even when the original selector never mentions the target. The
        // injected name flows through `plan`'s ordinary `escape` boundary
        // exactly like a parsed matcher (`tests/injection.rs`).
        let injected;
        let plan_expr = if q.target_labels.is_empty() {
            expr
        } else {
            injected = Expr::Log(inject_target_matchers(le, &q.target_labels));
            &injected
        };

        let ctx = self.config.plan_ctx(&self.tenant);
        // `limit`/`direction`/`step` are unused placeholders — volume
        // never reads samples through stage 3 (the `stats_inner` idiom).
        let qp = QueryParams {
            spec: QuerySpec::Range {
                start_ns: q.bounds.start_ns,
                end_ns: q.bounds.end_ns,
                step_ns: 1_000_000_000,
            },
            limit: 1,
            direction: Direction::Forward,
        };
        let sp = match plan::plan(plan_expr, &qp, &ctx)? {
            Plan::Streams(sp) => sp,
            // Unreachable (an `Expr::Log` always plans to `Streams`) but
            // kept as a structured rejection, never a panic.
            Plan::Metric(_) | Plan::MetricBinary(_) => {
                return Err(ReadError::PipelineInvalid {
                    reason: "volume requires a log stream selector query".to_string(),
                });
            }
        };

        if let Some(e) = explain.as_mut() {
            e.push("stage1_stream_resolution", sp.stage1_sql.clone(), None);
        }
        let fingerprints = self.resolve_fingerprints(&sp.stage1_sql).await?;
        if fingerprints.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(e) = explain.as_mut() {
            e.push(
                "stage2_hydration",
                super::sql::stage2(
                    &self.tenant,
                    &sp.streams_table,
                    &sql_literals(&fingerprints),
                ),
                None,
            );
        }
        let meta = self.hydrate(&sp.streams_table, &fingerprints).await?;

        let window = super::sql::TimeWindow {
            start_ns: q.bounds.start_ns,
            end_ns: q.bounds.end_ns,
        };
        let sql = super::sql::log_volume_rollup(
            &self.tenant,
            &self.config.rollup_table,
            &sql_literals(&fingerprints),
            window,
        );
        if let Some(e) = explain.as_mut() {
            let routing = super::plan::RoutingDecision {
                chosen: super::plan::RouteChoice::Rollup,
                reason: "rollup: volume accepts matchers-only queries — always served from the \
                         rollup with zero body reads"
                    .to_string(),
            };
            e.set_routing(routing.clone());
            e.push("volume_read", sql.clone(), Some(routing.reason));
        }

        let mut rows: Vec<VolumeRow> = Vec::new();
        {
            // Scoped so the stream's pooled-connection lease drops before
            // the (pure-CPU) accumulation below.
            let mut stream = self
                .query_stream::<VolumeRow>(&sql, &self.budget_settings())
                .await?;
            while let Some(row) = stream.next().await {
                rows.push(row.map_err(|e| {
                    map_read_error(
                        e,
                        self.config.scan_budget_bytes,
                        self.config.read_max_memory_bytes,
                    )
                })?);
            }
        }
        Ok(accumulate_volume(
            &rows,
            &meta,
            q.aggregate_by,
            &labels_to_match,
            !q.target_labels.is_empty(),
            q.limit,
        ))
    }
}

/// The label-name set a volume query keys on (issue #169, oracle
/// `PrepareLabelsAndMatchers`): the `targetLabels` set when supplied,
/// otherwise the selector's OWN matcher names — every op, including
/// `!=`/`!~` (the oracle adds every `m.Name`, so `{env!="dev"}` keys
/// results by each stream's `env` value).
fn volume_labels_to_match(selector: &StreamSelector, target_labels: &[String]) -> BTreeSet<String> {
    if target_labels.is_empty() {
        selector.matchers.iter().map(|m| m.name.clone()).collect()
    } else {
        target_labels.iter().cloned().collect()
    }
}

/// `targetLabels` matcher injection (issue #169, oracle
/// `prepareLabelsAndMatchersWithTargets`): each target with no matcher of
/// its name gets `name =~ ".+"` appended to the selector; targets already
/// matched (any op) are left alone. Pure — the caller plans the returned
/// expression, so the injected name crosses the ordinary `escape`
/// boundary like any parsed matcher.
fn inject_target_matchers(le: &LogExpr, target_labels: &[String]) -> LogExpr {
    let mut out = le.clone();
    for target in target_labels {
        if !out.selector.matchers.iter().any(|m| m.name == *target) {
            out.selector.matchers.push(Matcher {
                name: target.clone(),
                op: MatchOp::Re,
                value: ".+".to_string(),
            });
        }
    }
    out
}

/// Pure volume accumulation over the rollup rows (issue #169, oracle
/// `seriesvolume.Add`/`MapToVolumeResponse` + `instance.go getVolume`):
///
/// - **Series mode:** key = the stream's label pairs whose name is in
///   `labels_to_match`; bytes accumulate saturating. A stream matching
///   none of the names groups under the empty `{}` key.
/// - **Labels mode:** each label NAME of the stream — restricted to
///   `labels_to_match` when `restrict_label_names` (i.e. `targetLabels`
///   was supplied), otherwise ALL of the stream's names — accumulates
///   under the single-pair key `(name, "")`.
///
/// A stream with no rollup row in-window contributes nothing (the rows
/// slice simply lacks it); a returned `bytes = 0` row DOES contribute a
/// zero entry. Output sorted `(bytes desc, labels asc)` — the oracle's
/// value-desc/name-asc presentation — truncated to `limit`.
fn accumulate_volume(
    rows: &[VolumeRow],
    meta: &HashMap<Fingerprint, StreamMetaRow>,
    aggregate_by: VolumeAggregateBy,
    labels_to_match: &BTreeSet<String>,
    restrict_label_names: bool,
    limit: u32,
) -> Vec<VolumeEntry> {
    let mut acc: BTreeMap<Vec<(String, String)>, u64> = BTreeMap::new();
    for row in rows {
        // A rollup row whose fingerprint failed to hydrate (non-atomic
        // stream/sample writes) has no label set to key on — skip it, the
        // same tolerance stage 2's ReplacingMergeTree dedup documents.
        let Some(m) = meta.get(&row.fingerprint) else {
            continue;
        };
        let stream_labels = series_labels(m);
        match aggregate_by {
            VolumeAggregateBy::Series => {
                let key: Vec<(String, String)> = stream_labels
                    .into_iter()
                    .filter(|(name, _)| labels_to_match.contains(name))
                    .collect();
                let entry = acc.entry(key).or_insert(0);
                *entry = entry.saturating_add(row.bytes);
            }
            VolumeAggregateBy::Labels => {
                for (name, _) in stream_labels {
                    if restrict_label_names && !labels_to_match.contains(&name) {
                        continue;
                    }
                    let entry = acc.entry(vec![(name, String::new())]).or_insert(0);
                    *entry = entry.saturating_add(row.bytes);
                }
            }
        }
    }
    let mut out: Vec<VolumeEntry> = acc
        .into_iter()
        .map(|(labels, bytes)| VolumeEntry { labels, bytes })
        .collect();
    out.sort_by(|a, b| b.bytes.cmp(&a.bytes).then_with(|| a.labels.cmp(&b.labels)));
    out.truncate(limit as usize);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logql::testkit::*;

    // -- Issue #169: volume keying/aggregation, one test per oracle rule
    //    (grafana/loki:3.4.2 `PrepareLabelsAndMatchers`/`seriesvolume`) --

    fn vol_selector(matchers: &[(&str, MatchOp, &str)]) -> StreamSelector {
        StreamSelector {
            matchers: matchers
                .iter()
                .map(|(name, op, value)| Matcher {
                    name: name.to_string(),
                    op: *op,
                    value: value.to_string(),
                })
                .collect(),
        }
    }

    fn vol_names(list: &[&str]) -> BTreeSet<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    /// `(fingerprint, service, canonical labels JSON)` fixtures.
    fn vol_meta(entries: &[(u64, &str, &str)]) -> HashMap<Fingerprint, StreamMetaRow> {
        entries
            .iter()
            .map(|(fp, service, labels)| {
                (
                    Fingerprint::from_raw(u128::from(*fp)),
                    StreamMetaRow {
                        fingerprint: Fingerprint::from_raw(u128::from(*fp)),
                        service: service.to_string(),
                        labels: labels.to_string(),
                    },
                )
            })
            .collect()
    }

    fn vol_rows(list: &[(u64, u64)]) -> Vec<VolumeRow> {
        list.iter()
            .map(|(fingerprint, bytes)| VolumeRow {
                fingerprint: Fingerprint::from_raw(u128::from(*fingerprint)),
                bytes: *bytes,
            })
            .collect()
    }

    #[test]
    fn volume_labels_to_match_uses_every_matcher_name_including_negative_ops() {
        // Oracle rule: `PrepareLabelsAndMatchers` adds EVERY `m.Name` —
        // `{env!="dev"}` keys results by each stream's `env` value.
        let sel = vol_selector(&[
            ("service_name", MatchOp::Eq, "checkout"),
            ("env", MatchOp::Neq, "dev"),
            ("app", MatchOp::Nre, "test.*"),
        ]);
        assert_eq!(
            volume_labels_to_match(&sel, &[]),
            vol_names(&["service_name", "env", "app"])
        );
    }

    #[test]
    fn volume_labels_to_match_prefers_the_target_set_over_matcher_names() {
        let sel = vol_selector(&[("service_name", MatchOp::Eq, "checkout")]);
        let targets = vec!["env".to_string(), "team".to_string()];
        assert_eq!(
            volume_labels_to_match(&sel, &targets),
            vol_names(&["env", "team"])
        );
    }

    #[test]
    fn inject_target_matchers_appends_a_dot_plus_regex_only_for_absent_names() {
        let le = LogExpr {
            selector: vol_selector(&[("service_name", MatchOp::Eq, "checkout")]),
            pipeline: Vec::new(),
        };
        let out = inject_target_matchers(&le, &["env".to_string(), "service_name".to_string()]);
        // `service_name` already has a matcher (any op counts) — only
        // `env` gains the injected `=~ ".+"`.
        assert_eq!(out.selector.matchers.len(), 2);
        assert_eq!(
            out.selector.matchers[1],
            Matcher {
                name: "env".to_string(),
                op: MatchOp::Re,
                value: ".+".to_string(),
            }
        );
    }

    #[test]
    fn series_mode_keys_by_the_matched_name_subset_of_the_stream_labels() {
        let meta = vol_meta(&[(1, "checkout", r#"{"env":"prod","team":"pay"}"#)]);
        let out = accumulate_volume(
            &vol_rows(&[(1, 10)]),
            &meta,
            VolumeAggregateBy::Series,
            &vol_names(&["env"]),
            false,
            100,
        );
        // Only the matched name enters the key — `team`/`service_name`
        // are dropped from it.
        assert_eq!(
            out,
            vec![VolumeEntry {
                labels: pairs(&[("env", "prod")]),
                bytes: 10,
            }]
        );
    }

    #[test]
    fn series_mode_omits_an_absent_label_from_the_key() {
        // fp 2 has no `env` label: its key is the `service_name` pair
        // alone, never an empty-value `env` pair.
        let meta = vol_meta(&[
            (1, "checkout", r#"{"env":"prod"}"#),
            (2, "checkout", r#"{}"#),
        ]);
        let out = accumulate_volume(
            &vol_rows(&[(1, 10), (2, 20)]),
            &meta,
            VolumeAggregateBy::Series,
            &vol_names(&["env", "service_name"]),
            false,
            100,
        );
        assert_eq!(
            out,
            vec![
                VolumeEntry {
                    labels: pairs(&[("service_name", "checkout")]),
                    bytes: 20,
                },
                VolumeEntry {
                    labels: pairs(&[("env", "prod"), ("service_name", "checkout")]),
                    bytes: 10,
                },
            ]
        );
    }

    #[test]
    fn series_mode_groups_streams_matching_no_name_under_the_empty_key() {
        let meta = vol_meta(&[
            (1, "checkout", r#"{"env":"prod"}"#),
            (2, "billing", r#"{"env":"dev"}"#),
        ]);
        let out = accumulate_volume(
            &vol_rows(&[(1, 3), (2, 4)]),
            &meta,
            VolumeAggregateBy::Series,
            &vol_names(&["region"]),
            false,
            100,
        );
        // Neither stream carries `region`: both accumulate under `{}`.
        assert_eq!(
            out,
            vec![VolumeEntry {
                labels: Vec::new(),
                bytes: 7,
            }]
        );
    }

    #[test]
    fn labels_mode_uses_all_stream_names_when_no_targets_are_supplied() {
        let meta = vol_meta(&[
            (1, "checkout", r#"{"env":"prod"}"#),
            (2, "checkout", r#"{"env":"dev","team":"pay"}"#),
        ]);
        let out = accumulate_volume(
            &vol_rows(&[(1, 10), (2, 5)]),
            &meta,
            VolumeAggregateBy::Labels,
            &vol_names(&["service_name"]),
            false, // no targetLabels: every stream name counts
            100,
        );
        assert_eq!(
            out,
            vec![
                VolumeEntry {
                    labels: pairs(&[("env", "")]),
                    bytes: 15,
                },
                VolumeEntry {
                    labels: pairs(&[("service_name", "")]),
                    bytes: 15,
                },
                VolumeEntry {
                    labels: pairs(&[("team", "")]),
                    bytes: 5,
                },
            ]
        );
    }

    #[test]
    fn labels_mode_restricts_to_the_target_names_when_targets_are_supplied() {
        let meta = vol_meta(&[(1, "checkout", r#"{"env":"prod","team":"pay"}"#)]);
        let out = accumulate_volume(
            &vol_rows(&[(1, 10)]),
            &meta,
            VolumeAggregateBy::Labels,
            &vol_names(&["env"]),
            true, // targetLabels supplied: only the target names count
            100,
        );
        assert_eq!(
            out,
            vec![VolumeEntry {
                labels: pairs(&[("env", "")]),
                bytes: 10,
            }]
        );
    }

    #[test]
    fn volume_entries_sort_bytes_desc_with_label_asc_tie_break() {
        let meta = vol_meta(&[
            (1, "checkout", r#"{"env":"zeta"}"#),
            (2, "checkout", r#"{"env":"alpha"}"#),
            (3, "checkout", r#"{"env":"big"}"#),
        ]);
        let out = accumulate_volume(
            &vol_rows(&[(1, 5), (2, 5), (3, 9)]),
            &meta,
            VolumeAggregateBy::Series,
            &vol_names(&["env"]),
            false,
            100,
        );
        // Bytes-desc first (big=9), then the 5-byte tie breaks label-asc
        // (alpha before zeta) — NEVER a plain label sort.
        assert_eq!(
            out.iter().map(|e| &e.labels[0].1).collect::<Vec<_>>(),
            vec!["big", "alpha", "zeta"]
        );
    }

    #[test]
    fn volume_limit_truncates_after_the_sort() {
        let meta = vol_meta(&[
            (1, "checkout", r#"{"env":"small"}"#),
            (2, "checkout", r#"{"env":"large"}"#),
        ]);
        let out = accumulate_volume(
            &vol_rows(&[(1, 1), (2, 100)]),
            &meta,
            VolumeAggregateBy::Series,
            &vol_names(&["env"]),
            false,
            1,
        );
        // limit=1 keeps the LARGER entry — truncation runs post-sort.
        assert_eq!(
            out,
            vec![VolumeEntry {
                labels: pairs(&[("env", "large")]),
                bytes: 100,
            }]
        );
    }

    #[test]
    fn a_zero_byte_rollup_row_still_contributes_a_zero_entry() {
        // A returned row with bytes = 0 contributes a 0 entry (a stream
        // with NO row contributes nothing — it is simply absent here).
        let meta = vol_meta(&[(1, "checkout", r#"{"env":"prod"}"#)]);
        let out = accumulate_volume(
            &vol_rows(&[(1, 0)]),
            &meta,
            VolumeAggregateBy::Series,
            &vol_names(&["env"]),
            false,
            100,
        );
        assert_eq!(
            out,
            vec![VolumeEntry {
                labels: pairs(&[("env", "prod")]),
                bytes: 0,
            }]
        );
    }

    #[test]
    fn volume_accumulation_saturates_instead_of_wrapping() {
        let meta = vol_meta(&[
            (1, "checkout", r#"{"env":"prod"}"#),
            (2, "checkout", r#"{"env":"prod"}"#),
        ]);
        let out = accumulate_volume(
            &vol_rows(&[(1, u64::MAX), (2, 2)]),
            &meta,
            VolumeAggregateBy::Series,
            &vol_names(&["env"]),
            false,
            100,
        );
        assert_eq!(out[0].bytes, u64::MAX, "saturating, never wrapping");
    }
}
