use super::*;

/// The third statement's predicate list: the plan's line filters, plus
/// every parsed-name filter whose proviso holds over the streams this
/// request actually resolved (issue #507, W3, arm 3).
///
/// **This is where a cell that is sound GIVEN NAMED STATE has its proviso
/// checked.** A parsed-name fragment reads a key of the line, which is the
/// label the evaluator resolved only while no selected stream carries a
/// label of that name. Where one does, the stream label wins the collision
/// and the line's key is renamed, so the fragment would compare the wrong
/// value and drop a row the evaluator keeps.
///
/// It is checked HERE, and nowhere else, because a stream label is
/// constant across every row the statement reads: no per-row predicate can
/// test it, and it is not knowable before stage 2 resolves the label sets.
/// The other half of the same hazard — a structured-metadata key of the
/// same name — is per row, and the fragment's own whole-value guard tests
/// it inside the statement. Two facts with two lifetimes, each checked in
/// the one place it is knowable.
///
/// **Every call site has already run stage 2**, so the proviso costs no
/// round trip, and EXPLAIN reports the statement execution issues because
/// both go through here.
///
/// **Why the fragments are minted here rather than carried on the plan**,
/// which is bookkeeping rather than design: forty-six design-record
/// citations name a bare `plan.rs:<line>` and are FROZEN as unresolvable,
/// because the basename matches several tracked files. A frozen row
/// carries no file, so a field added to `StreamsPlan` would move lines
/// those citations point at and no rule could repair them. The text is
/// still produced by one function — `plan::compile_parsed_label_filters`,
/// over the plan's own pipeline — so there is no second renderer.
pub(super) fn stage3_predicates(
    sp: &StreamsPlan,
    fingerprints: &[Fingerprint],
    meta: &HashMap<Fingerprint, StreamMetaRow>,
    budget: usize,
) -> LoweredPredicates {
    let mut out = sp.line_filters.clone();
    let candidates = super::plan::compile_parsed_label_filters(&sp.pipeline);
    if !candidates.is_empty() {
        // One pass over the stream labels serves the name check and every
        // stream guard the candidates ask for (issue #624, part 3d-3): a
        // stream carrying a name's stem label lands a hit on the stem
        // under `<stem>_extracted`, so its rows are kept whole.
        let stems: BTreeSet<&str> = candidates
            .iter()
            .filter_map(|p| p.stream_guard.as_deref())
            .collect();
        let mut stream_label_names: BTreeSet<String> = BTreeSet::new();
        let mut guarded: HashMap<&str, Vec<pulsus_model::FpLiteral>> = HashMap::new();
        for (fp, m) in meta {
            for (k, _) in series_labels(m) {
                if let Some(stem) = stems.get(k.as_str()) {
                    guarded.entry(stem).or_default().push(fp.sql_literal());
                }
                stream_label_names.insert(k);
            }
        }
        for fps in guarded.values_mut() {
            fps.sort();
        }
        for pred in &candidates {
            if stream_label_names.contains(&pred.name) {
                continue;
            }
            match pred
                .stream_guard
                .as_deref()
                .and_then(|stem| guarded.get(stem))
            {
                Some(fps) => out.push(super::predicate::guard_streams(fps, &pred.fragment)),
                None => out.push(pred.fragment.clone()),
            }
        }
    }
    // Issue #544: the structured-metadata filters, rendered from the
    // stream label sets stage 2 has just resolved. `None` is the budget
    // fallback — the query takes today's route with today's answer.
    match metadata_predicates(&sp.pipeline, fingerprints, meta, budget) {
        Some(fragments) => {
            out.extend(fragments);
            LoweredPredicates {
                predicates: out,
                metadata_lowered: true,
            }
        }
        None => LoweredPredicates {
            predicates: out,
            metadata_lowered: !plans_a_metadata_filter(&sp.pipeline),
        },
    }
}
/// The stage-3 predicate vector, and whether the structured-metadata
/// filters the plan counted on are in it (issue #544).
///
/// `metadata_lowered` is `false` only when the plan lowered a metadata
/// filter and the render did not fit
/// [`super::predicate::MAX_METADATA_FRAGMENT_BYTES`]. The caller then
/// takes the route it takes today: the answer is the same and the cost is
/// the one it has now. **Never a rejection** — a request that is legal
/// today stays legal.
pub(super) struct LoweredPredicates {
    pub(super) predicates: Vec<CheckedFragment>,
    pub(super) metadata_lowered: bool,
}

/// One stream's exposed label set, by fingerprint — parsed once per read
/// rather than once per filter leaf.
fn label_sets(
    fingerprints: &[Fingerprint],
    meta: &HashMap<Fingerprint, StreamMetaRow>,
) -> Option<HashMap<Fingerprint, Vec<(String, String)>>> {
    let mut out = HashMap::with_capacity(fingerprints.len());
    for fp in fingerprints {
        // A selected fingerprint stage 2 did not hydrate has no label set,
        // so no class can be decided for it. The read then takes today's
        // route, where its rows are dropped by the client as they are
        // today.
        let m = meta.get(fp)?;
        out.insert(*fp, series_labels(m));
    }
    Some(out)
}

/// Which name-resolution class each selected fingerprint falls in, for one
/// filter leaf (issue #544). See
/// [`super::predicate::MetadataNameClasses`] for the rule.
struct NameClasses {
    stream_label: Vec<Fingerprint>,
    stream_label_true: Vec<Fingerprint>,
    unsuffixed: Vec<Fingerprint>,
    direct: Vec<Fingerprint>,
    base_name: Option<String>,
}

fn classify_metadata_name(
    name: &str,
    op: MatchOp,
    value: &str,
    fingerprints: &[Fingerprint],
    labels: &HashMap<Fingerprint, Vec<(String, String)>>,
) -> Option<NameClasses> {
    let base = name.strip_suffix("_extracted");
    let mut classes = NameClasses {
        stream_label: Vec::new(),
        stream_label_true: Vec::new(),
        unsuffixed: Vec::new(),
        direct: Vec::new(),
        base_name: None,
    };
    for fp in fingerprints {
        let Some(set) = labels.get(fp) else {
            // `label_sets` refuses a missing fingerprint before this runs.
            continue;
        };
        let carries_name = set.iter().any(|(k, _)| k == name);
        let carries_base = base.is_some_and(|m| set.iter().any(|(k, _)| k == m));
        if carries_name && carries_base {
            // **The DOUBLE collision, which the three classes do not
            // cover** — measured, `crates/pulsus-server/tests/loki_push_live.rs`'s
            // `structured_metadata_double_collision_overwrites_the_extracted_slot_once`.
            //
            // The stream carries BOTH `k` and `k` without its
            // `_extracted` suffix as labels. A metadata pair named by the
            // un-suffixed one is renamed to `k` and then **overwrites the
            // stream label of that name** — `merge_metadata_pairs`
            // (`crates/pulsus-read/src/logql/labels.rs`) assigns into the
            // slot it finds, base region included. So on such a stream the
            // stream label does NOT win, which is what the stream-label
            // class is for, and the value depends per row on whether the
            // metadata carries the un-suffixed name.
            //
            // Rendering it needs a fourth arm — a per-fingerprint constant
            // under a `JSONHas` — and this work states three. The filter
            // therefore does not lower and the query takes the route it
            // takes today, with today's answer: the same fallback an
            // oversized fragment takes.
            return None;
        }
        if carries_name {
            classes.stream_label.push(*fp);
            let verdict = match op {
                MatchOp::Eq => set.iter().any(|(k, v)| k == name && v == value),
                MatchOp::Neq => set.iter().any(|(k, v)| k == name && v != value),
                // Refused at the plan-time servability check.
                MatchOp::Re | MatchOp::Nre => false,
            };
            if verdict {
                classes.stream_label_true.push(*fp);
            }
        } else if carries_base {
            classes.unsuffixed.push(*fp);
        } else {
            classes.direct.push(*fp);
        }
    }
    if !classes.unsuffixed.is_empty() {
        classes.base_name = base.map(str::to_string);
    }
    Some(classes)
}

/// One label-filter stage's whole tree as one fragment (issue #544).
///
/// **Iterative** (issue #272): a flat `a or b or c …` chain parses into a
/// LEFT-DEEP tree of unbounded depth, so this walks a post-order node list
/// with an explicit operand stack. `remaining` is the statement's whole
/// metadata budget and is spent leaf by leaf, so three filters cannot each
/// claim the budget in full.
fn render_metadata_tree(
    expr: &LabelFilterExpr,
    fingerprints: &[Fingerprint],
    labels: &HashMap<Fingerprint, Vec<(String, String)>>,
    remaining: &mut usize,
) -> Option<CheckedFragment> {
    let mut nodes: Vec<&LabelFilterExpr> = Vec::new();
    if matches!(expr, LabelFilterExpr::And(_, _) | LabelFilterExpr::Or(_, _)) {
        pulsus_logql::walk::postorder_into::<pulsus_logql::LabelFilterScc>(expr, &mut nodes);
    } else {
        nodes.push(expr);
    }
    let mut stack: Vec<CheckedFragment> = Vec::with_capacity(nodes.len());
    for n in nodes {
        match n {
            LabelFilterExpr::Match(m) => {
                let c = classify_metadata_name(&m.name, m.op, &m.value, fingerprints, labels)?;
                let (fragment, _complement) = super::predicate::metadata_string_filter(
                    &m.name,
                    m.op,
                    &m.value,
                    super::predicate::MetadataNameClasses {
                        selected: &sql_literals(fingerprints),
                        stream_label: &sql_literals(&c.stream_label),
                        stream_label_true: &sql_literals(&c.stream_label_true),
                        unsuffixed: &sql_literals(&c.unsuffixed),
                        direct: &sql_literals(&c.direct),
                        base_name: c.base_name.as_deref(),
                    },
                    *remaining,
                )
                .ok()?;
                *remaining = remaining.checked_sub(fragment.as_sql().len())?;
                stack.push(fragment);
            }
            // Refused at the plan-time servability check, so the stage
            // would not be in the plan at all.
            LabelFilterExpr::Compare { .. } | LabelFilterExpr::Ip { .. } => return None,
            LabelFilterExpr::And(_, _) | LabelFilterExpr::Or(_, _) => {
                let rhs = stack.pop()?;
                let lhs = stack.pop()?;
                let joined = if matches!(n, LabelFilterExpr::And(_, _)) {
                    super::predicate::metadata_filter_and(&lhs, &rhs)
                } else {
                    super::predicate::metadata_filter_or(&lhs, &rhs)
                };
                // The two operands are already charged; only the four
                // bytes of parentheses and the operator are new.
                let added = joined.as_sql().len() - lhs.as_sql().len() - rhs.as_sql().len();
                *remaining = remaining.checked_sub(added)?;
                stack.push(joined);
            }
        }
    }
    match stack.len() {
        1 => stack.pop(),
        _ => None,
    }
}

/// Every lowered structured-metadata fragment for one read, or `None`
/// when the render does not fit the budget (issue #544).
fn metadata_predicates(
    pipeline: &[Stage],
    fingerprints: &[Fingerprint],
    meta: &HashMap<Fingerprint, StreamMetaRow>,
    budget: usize,
) -> Option<Vec<CheckedFragment>> {
    let plan = super::plan::compile_metadata_label_filters(pipeline)?;
    metadata_predicates_of(&plan.stages, fingerprints, meta, budget)
}

/// The same, for a plan that carries the trees rather than the pipeline —
/// the two metric routes on which the aggregate is the database's and no
/// client stage holds a pipeline to read them off.
fn metadata_predicates_of(
    stages: &[Stage],
    fingerprints: &[Fingerprint],
    meta: &HashMap<Fingerprint, StreamMetaRow>,
    budget: usize,
) -> Option<Vec<CheckedFragment>> {
    let labels = label_sets(fingerprints, meta)?;
    let mut remaining = budget;
    let mut out = Vec::with_capacity(stages.len());
    for stage in stages {
        let Stage::LabelFilter(tree) = stage else {
            // `compile_metadata_label_filters` puts nothing else here.
            return None;
        };
        out.push(render_metadata_tree(
            tree,
            fingerprints,
            &labels,
            &mut remaining,
        )?);
    }
    Some(out)
}

/// Whether this pipeline has a metadata filter the PLAN lowered — the
/// question `metadata_lowered == false` answers against.
fn plans_a_metadata_filter(pipeline: &[Stage]) -> bool {
    super::plan::compile_metadata_label_filters(pipeline).is_some()
}

/// The metric read's predicate vector, and whether the structured-metadata
/// filters the plan counted on are in it (issue #544).
///
/// Called on the two routes where the aggregate is the DATABASE's: an
/// instant count/bytes query with no unwrap and no grouping, and a range
/// query whose `[range]` equals its `step`. On those two a missing
/// fragment is a wrong number, not a wider scan, so `metadata_lowered ==
/// false` is what makes the caller swap the restored client stage in.
pub(super) fn metric_predicates(
    mp: &MetricPlan,
    fingerprints: &[Fingerprint],
    meta: &HashMap<Fingerprint, StreamMetaRow>,
    budget: usize,
) -> LoweredPredicates {
    let mut out = mp.extra_predicates.clone();
    let Some(lowering) = &mp.metadata_lowering else {
        return LoweredPredicates {
            predicates: out,
            metadata_lowered: true,
        };
    };
    match metadata_predicates_of(&lowering.stages, fingerprints, meta, budget) {
        Some(fragments) => {
            out.extend(fragments);
            LoweredPredicates {
                predicates: out,
                metadata_lowered: true,
            }
        }
        None => LoweredPredicates {
            predicates: out,
            metadata_lowered: false,
        },
    }
}
