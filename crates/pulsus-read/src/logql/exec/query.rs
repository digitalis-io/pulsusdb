use super::*;

impl LogQlEngine {
    /// Returns the result alongside the [`Warnings`] the evaluation
    /// accumulated (issue #277) — empty unless a `variants(...)` query
    /// skipped a variant or a drilldown request was cut to the series cap
    /// (issue #605); an empty accumulator renders zero bytes on the wire.
    pub async fn query(
        &self,
        expr: &Expr,
        params: &QueryParams,
    ) -> Result<(QueryResult, Warnings), ReadError> {
        self.query_with(expr, params, ResponseOptions::default())
            .await
    }

    /// [`Self::query`] with the response-shape options the request asked
    /// for (issue #463). `ResponseOptions::default()` is the pre-#463
    /// behaviour exactly.
    pub async fn query_with(
        &self,
        expr: &Expr,
        params: &QueryParams,
        opts: ResponseOptions,
    ) -> Result<(QueryResult, Warnings), ReadError> {
        let ctx = self.config.plan_ctx(&self.tenant);
        let mut warnings = Warnings::new();
        match plan::plan(expr, params, &ctx)? {
            Plan::Streams(sp) => self
                .run_streams_inner(&sp, None, opts)
                .await
                .map(|(items, partial)| (QueryResult::Streams { items, partial }, warnings)),
            // Issue #236: [`MAX_QUERY_SERIES`] is a FINAL-RESULT cap, so it
            // is applied here — on the whole expression's output — and
            // never inside `run_metric_inner`/`run_metric_node`/
            // `apply_vector_aggs`, where it would reject on scanned or
            // intermediate groups the reference never counts.
            Plan::Metric(mp) => {
                let result = self.run_metric_inner(&mp, None).await?;
                let result = cap_result_series(result, opts, expr, params, &mut warnings)?;
                Ok((result, warnings))
            }
            Plan::MetricBinary(node) => {
                let mut result = self.run_metric_node(&node, None, &mut warnings).await?;
                if final_series_gate_applies(&node) {
                    result = cap_result_series(result, opts, expr, params, &mut warnings)?;
                }
                Ok((result, warnings))
            }
        }
    }

    /// One execution that also captures the plan trace (#13's
    /// `X-Pulsus-Explain`) — `run_streams_inner`/`run_metric_inner` push
    /// every stage's SQL into `explain` in the same single pass that
    /// executes it, so this incurs **zero** extra ClickHouse reads versus
    /// [`LogQlEngine::query`] (architect plan amendment §3, resolving the
    /// round-1 review finding that a naive `query()` + `explain()` pairing
    /// would double-execute and could observe different data).
    pub async fn query_explained(
        &self,
        expr: &Expr,
        params: &QueryParams,
    ) -> Result<(QueryResult, Warnings, PlanExplain), ReadError> {
        self.query_explained_with(expr, params, ResponseOptions::default())
            .await
    }

    /// [`Self::query_explained`] with the response-shape options the
    /// request asked for (issue #463).
    pub async fn query_explained_with(
        &self,
        expr: &Expr,
        params: &QueryParams,
        opts: ResponseOptions,
    ) -> Result<(QueryResult, Warnings, PlanExplain), ReadError> {
        let ctx = self.config.plan_ctx(&self.tenant);
        let mut warnings = Warnings::new();
        match plan::plan(expr, params, &ctx)? {
            Plan::Streams(sp) => {
                let mut explain = PlanExplain::new("streams");
                let (items, partial) = self
                    .run_streams_inner(&sp, Some(&mut explain), opts)
                    .await?;
                Ok((QueryResult::Streams { items, partial }, warnings, explain))
            }
            Plan::Metric(mp) => {
                let result_type = if mp.answers_instant() {
                    "vector"
                } else {
                    "matrix"
                };
                let mut explain = PlanExplain::new(result_type);
                let result = self.run_metric_inner(&mp, Some(&mut explain)).await?;
                let result = cap_result_series(result, opts, expr, params, &mut warnings)?;
                Ok((result, warnings, explain))
            }
            Plan::MetricBinary(node) => {
                let mut explain = PlanExplain::new(binary_result_type(&node, params));
                let mut result = self
                    .run_metric_node(&node, Some(&mut explain), &mut warnings)
                    .await?;
                if final_series_gate_applies(&node) {
                    result = cap_result_series(result, opts, expr, params, &mut warnings)?;
                }
                Ok((result, warnings, explain))
            }
        }
    }

    /// Evaluates a [`MetricNode`] tree (issue M6-10): leaves execute the
    /// ordinary metric path; `Binary`/`Scalar`/`VectorAgg` combine the
    /// results in-engine.
    ///
    /// **Issue #272: iterative.** The tree is flattened to a post-order
    /// node list on the heap and folded over a value stack reserved once
    /// at the exact high-water mark, so a wide `a or b or c …` chain — a
    /// LEFT-DEEP tree at parse depth 1 — no longer costs a machine frame
    /// per term. The `Pin<Box<dyn Future>>` return type is deleted with
    /// the recursion it existed for.
    ///
    /// Evaluation order is unchanged: post-order left-to-right visits
    /// `lhs`'s whole subtree before `rhs`'s, the first error
    /// short-circuits, and `explain` accumulates leaves left-to-right.
    async fn run_metric_node(
        &self,
        node: &MetricNode,
        explain: Option<&mut PlanExplain>,
        warnings: &mut Warnings,
    ) -> Result<QueryResult, ReadError> {
        // Issue #272, memory L5 Leg B: EVERY allocating step is charged
        // BEFORE it happens, against the same derived cap Leg A uses —
        // the post-order walk's own work stack and node vector inside
        // `metric_node_postorder_charged`, then the value stack here. A
        // failed charge does not mutate the counter and stops the walk
        // before the allocation it refused; the breach is a clean
        // `QueryTooBroad(WalkTransientBytes)` -> 422, never an abort.
        let mut budget = super::walkbound::WalkBudget::new();
        let (nodes, max_value_stack) = plan::metric_node_postorder_charged(node, &mut budget)?;
        budget.charge(max_value_stack.saturating_mul(size_of::<QueryResult>()))?;
        let mut explain = explain;
        let mut vals: Vec<QueryResult> = Vec::with_capacity(max_value_stack);
        for n in nodes {
            let v = match n {
                MetricNode::Leaf(mp) => self.run_metric_inner(mp, explain.as_deref_mut()).await?,
                MetricNode::Scalar(v) => QueryResult::Scalar(*v),
                MetricNode::VectorLit { value, window } => materialize_vector_lit(*value, window)?,
                MetricNode::VectorAgg { aggs, .. } => {
                    let inner = pop_value(&mut vals);
                    apply_vector_aggs(inner, aggs)?
                }
                MetricNode::LabelReplace { spec, .. } => {
                    let inner = pop_value(&mut vals);
                    apply_label_replace(inner, spec)?
                }
                MetricNode::Variants {
                    scan,
                    variants,
                    spec_bytes,
                } => {
                    self.run_variants(
                        scan,
                        variants,
                        *spec_bytes,
                        explain.as_deref_mut(),
                        warnings,
                    )
                    .await?
                }
                MetricNode::Binary {
                    op,
                    return_bool,
                    matching,
                    ..
                } => {
                    // Post-order leaves [.., lhs, rhs] on the tail.
                    let r = pop_value(&mut vals);
                    let l = pop_value(&mut vals);
                    combine_binary(*op, *return_bool, matching.as_ref(), l, r)?
                }
            };
            vals.push(v);
        }
        match vals.pop() {
            Some(v) if vals.is_empty() => Ok(v),
            // Unreachable: a post-order fold consumes exactly `arity(n)`
            // values per node and pushes one, so exactly one survives.
            _ => unreachable!("a post-order metric fold leaves exactly one root value"),
        }
    }
}

/// Issue #277: does the FINAL-result series cap apply to this root?
///
/// **No, for a root `variants(...)`, and only there.** Measured against
/// the pinned reference: it dispatches on the ROOT expression
/// (`pkg/logql/engine.go:321-322 @ grafana/loki v3.7.4
/// b318f2829f0ae2094ab3a1e90780450e9e4b03be` — "A VariantsExpr is a
/// specific type of SampleExpr, so make sure this case is evaluated
/// first") into `evalVariants` → `JoinMultiVariantSampleVector`
/// (`:608-660`), which caps PER VARIANT and never the concatenation. Every
/// other root — including `variants(...) + 1` — goes to `JoinSampleVector`
/// (`:520-607`) and its plain cap at `:538`/`:588`.
///
/// Captured 2026-08-12 on that container: a root variants query returning
/// **501 total** series across two variants is `200` with no warning,
/// while the identical expression `+ 1` is `400` with
/// `maximum number of series (500) reached for a single query; …`.
///
/// Without this exemption the per-variant gate #236 landed is SHADOWED:
/// `exec.rs` capped the concatenation unconditionally, so a three-variant
/// query returning 400 series each — every variant under the cap — was a
/// 422.
/// `pub` so the conformance runner (`tests/logqltest/runner.rs`) applies
/// the IDENTICAL root test on its own `Plan::MetricBinary` arm rather
/// than a hand-copied twin of it — the same reason
/// [`ensure_result_series`](super::charge::ensure_result_series) is
/// exported.
pub fn final_series_gate_applies(node: &MetricNode) -> bool {
    !matches!(node, MetricNode::Variants { .. })
}

/// The `resultType` a binary metric plan produces: `scalar` for a tree
/// that produces no series (pure-literal, e.g. `5`/`5+3`), otherwise
/// vector/matrix per the query spec — the same rule the encoder applies to
/// the evaluated result. A `vector(n)` leaf (issue #221) produces a series
/// (`{} => n`), so `produces_series()` classifies a vector-lit-bearing tree
/// as vector/matrix even though it has no [`MetricNode::Leaf`].
pub(super) fn binary_result_type(node: &MetricNode, params: &QueryParams) -> &'static str {
    if !node.produces_series() {
        "scalar"
    } else if matches!(params.spec, QuerySpec::Instant { .. }) {
        "vector"
    } else {
        "matrix"
    }
}

/// Pops one operand off `run_metric_node`'s value stack.
#[inline]
fn pop_value(vals: &mut Vec<QueryResult>) -> QueryResult {
    match vals.pop() {
        Some(v) => v,
        // Unreachable: post-order guarantees a node's children have
        // already pushed their values.
        None => unreachable!("a post-order metric fold has its children's values on the stack"),
    }
}
