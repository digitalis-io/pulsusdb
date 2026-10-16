//! The search pipeline as one ordered fold (issue #602 part 2a,
//! `docs/api.md` §4.2): every `|` stage at its written position, inside the
//! one search statement.
//!
//! A query's stages split at each `coalesce()` into segments. Within a
//! segment a span's spanset is fixed by the values of the `by()` keys the
//! segment has written, so an aggregate's value is a property of its prefix
//! group and does not depend on the aggregates before it, which decide only
//! whether the group survives. The fold is three levels: the segment's
//! fine groups (`GROUP BY trace_id` and every key the segment writes), each
//! aggregate merged over its prefix (a window over the fine groups, or the
//! group itself when the prefix is every key), and the trace. A segment
//! followed by `coalesce()` hands its spans on through `ARRAY JOIN`, each
//! marked with whether it survived; one without an aggregate hands them on
//! as a condition.

use pulsus_traceql::AggregateOp;

use super::predicate::SpanPredicate;
use super::projection::GroupKeySql;

/// One `|` stage, by index into [`Fold`]'s lists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    Filter(usize),
    By(usize),
    Agg(usize),
    Coalesce,
}

/// One `by()` key.
#[derive(Debug, Clone)]
pub struct Key {
    pub display: String,
    pub sql: GroupKeySql,
    /// Whether the key's value is one per trace (`traceDuration`,
    /// `rootName`, `rootServiceName`): it never divides a trace.
    pub per_trace: bool,
}

/// What an aggregate reads.
#[derive(Debug, Clone)]
pub enum AggArg {
    Count,
    Duration,
    /// `v` a `Nullable(Float64)` over the span row, `kind` its stored type.
    Attr {
        v: String,
        kind: String,
    },
}

/// One aggregate stage.
#[derive(Debug, Clone)]
pub struct Agg {
    pub display: String,
    pub op: AggregateOp,
    pub arg: AggArg,
    pub sql_op: &'static str,
    /// `toFloat64('<threshold>')`.
    pub threshold: String,
}

/// A query's `|` stages, compiled.
#[derive(Debug, Clone, Default)]
pub struct Fold {
    pub filters: Vec<SpanPredicate>,
    pub keys: Vec<Key>,
    pub aggs: Vec<Agg>,
    pub stages: Vec<Stage>,
}

/// One response attribute's source, in the last segment's written order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Contributor {
    pub display: String,
    pub aggregate: bool,
}

/// Where the fold's first level reads its span rows: the span table, with
/// every column an expression, or a relation that names them.
pub struct Src {
    scope: String,
    from: String,
    inline: Vec<(String, String)>,
}

impl Src {
    /// The span table: `from` is `<table>\nWHERE …`, `scope` the numbering's
    /// `WITH` prefix, `inline` each named column's expression.
    pub fn table(scope: &str, from: String, inline: Vec<(String, String)>) -> Self {
        Src {
            scope: scope.to_string(),
            from,
            inline,
        }
    }

    /// A relation exposing every column by name.
    pub fn rel(rel: &str) -> Self {
        Src {
            scope: String::new(),
            from: format!("({rel})"),
            inline: Vec::new(),
        }
    }

    fn col(&self, name: &str) -> String {
        match self.inline.iter().find(|(n, _)| n == name) {
            Some((_, e)) => format!("({e})"),
            None => name.to_string(),
        }
    }

    /// `name` in a select list: its expression aliased, or the name.
    fn named(&self, name: &str) -> String {
        match self.inline.iter().find(|(n, _)| n == name) {
            Some((_, e)) => format!("({e}) AS {name}"),
            None => name.to_string(),
        }
    }
}

const BUCKET: &str = "intDiv(start_ns, 300000000000)";

fn fold_sum(list: &str) -> String {
    format!("arrayFold((acc, e) -> acc + e.1, arraySort(e -> (e.2, e.3), {list}), toFloat64('-0'))")
}

impl Fold {
    /// Whether no stage but `coalesce()` was written.
    pub fn is_empty(&self) -> bool {
        self.stages.iter().all(|s| *s == Stage::Coalesce)
    }

    fn segments(&self) -> Vec<&[Stage]> {
        self.stages.split(|s| *s == Stage::Coalesce).collect()
    }

    fn last_segment(&self) -> &[Stage] {
        self.segments().pop().unwrap_or(&[])
    }

    /// Whether a trace's survival reads more than its selector: a later
    /// filter or an aggregate anywhere in the pipeline.
    pub fn filters_traces(&self) -> bool {
        self.stages
            .iter()
            .any(|s| matches!(s, Stage::Filter(_) | Stage::Agg(_)))
    }

    /// The response attributes of every surviving spanset, in written
    /// order: the last segment's `by()` keys and aggregates. Empty when the
    /// response is flat.
    pub fn contributors(&self) -> Vec<Contributor> {
        self.last_segment()
            .iter()
            .filter_map(|s| match s {
                Stage::By(j) => Some(Contributor {
                    display: self.keys[*j].display.clone(),
                    aggregate: false,
                }),
                Stage::Agg(i) => Some(Contributor {
                    display: self.aggs[*i].display.clone(),
                    aggregate: true,
                }),
                _ => None,
            })
            .collect()
    }

    /// Whether an aggregate written after key `j`, in its segment,
    /// partitions by it: only then does the top-K read the key.
    pub fn key_matters(&self, j: usize) -> bool {
        for seg in self.segments() {
            if let Some(at) = seg.iter().position(|s| *s == Stage::By(j)) {
                return seg[at..].iter().any(|s| matches!(s, Stage::Agg(_)));
            }
        }
        false
    }

    /// Whether the top-K reads key `j` through a lookup scalar of its own.
    pub fn top_reads_lookup(&self, j: usize) -> bool {
        let k = &self.keys[j];
        k.sql.values.is_some() && !k.per_trace && self.key_matters(j)
    }

    /// The span columns the stages read, `(name, expression)`: each later
    /// filter `q<i>`, each key `k<j>` and its type `t<j>`, each attribute
    /// aggregate's value `a<i>` and stored type `ak<i>`. The top-K reads a
    /// key no aggregate partitions by, and a per-trace key, as `''`; a
    /// looked-up key reads `top_group_values<j>` there and
    /// `group_values<j>` in the detail.
    pub fn columns(&self, top: bool) -> Vec<(String, String)> {
        let mut out = Vec::new();
        for (i, f) in self.filters.iter().enumerate() {
            out.push((format!("q{i}"), f.sql().to_string()));
        }
        for (j, k) in self.keys.iter().enumerate() {
            if top && (!self.key_matters(j) || k.per_trace) {
                out.push((format!("k{j}"), "''".to_string()));
                out.push((format!("t{j}"), "''".to_string()));
                continue;
            }
            let alias = if top {
                format!("top_group_values{j}")
            } else {
                format!("group_values{j}")
            };
            out.push((format!("k{j}"), k.sql.value.replace("group_values", &alias)));
            out.push((
                format!("t{j}"),
                k.sql
                    .value_type
                    .as_deref()
                    .unwrap_or("''")
                    .replace("group_values", &alias),
            ));
        }
        for (i, a) in self.aggs.iter().enumerate() {
            if let AggArg::Attr { v, kind } = &a.arg {
                out.push((format!("a{i}"), v.clone()));
                out.push((format!("ak{i}"), kind.clone()));
            }
        }
        out
    }

    fn column_names(&self) -> Vec<String> {
        self.columns(false).into_iter().map(|(n, _)| n).collect()
    }

    /// The fold over `src`'s span rows: `trace_id`, `span_id`, `start_ns`,
    /// `duration_ns`, `s0` (the selector holds), in the detail `service`
    /// and `proj`, and [`Self::columns`]. `top` gives `(trace_id, last,
    /// keys)` for the newest `limit` surviving traces; otherwise `(trace_id,
    /// last, matched, spans[, groups])` for every trace `src` reads.
    /// `min_last`, for a top-K that reads a slice's candidates, keeps only
    /// the survivors whose order key is in the slice: a candidate's order
    /// key can lie outside it when the span that made it a candidate is not
    /// a member (a tree leaf under a failed `&&` guard).
    pub fn render(
        &self,
        src: Src,
        top: bool,
        limit: u32,
        spss: u32,
        min_last: Option<i64>,
    ) -> String {
        let in_slice = min_last.map_or_else(String::new, |n| format!(" AND last >= {n}"));
        let segments = self.segments();
        let (last, earlier) = segments.split_last().expect("split yields one segment");
        let mut src = src;
        let mut alive = src.col("s0");
        for seg in earlier {
            if !seg.iter().any(|s| matches!(s, Stage::Agg(_))) {
                for s in *seg {
                    if let Stage::Filter(i) = s {
                        alive = format!("{alive} AND {}", src.col(&format!("q{i}")));
                    }
                }
                continue;
            }
            let rel = self.explode(seg, &src, &alive, top);
            src = Src::rel(&rel);
            alive = "v".to_string();
        }
        let order = format!("ORDER BY last DESC, trace_id ASC\nLIMIT {limit}");
        let grouped = last
            .iter()
            .any(|s| matches!(s, Stage::By(_) | Stage::Agg(_)));
        if !grouped {
            let mut end = alive;
            for s in *last {
                if let Stage::Filter(i) = s {
                    end = format!("{end} AND {}", src.col(&format!("q{i}")));
                }
            }
            let s0 = src.col("s0");
            if top {
                return format!(
                    "{}SELECT trace_id, maxIf(start_ns, {s0}) AS last, groupUniqArray({BUCKET}) AS keys\n\
                     FROM {}\nGROUP BY trace_id\nHAVING countIf({end}) > 0{in_slice}\n{order}",
                    src.scope, src.from
                );
            }
            return format!(
                "{}SELECT trace_id, maxIf(start_ns, {s0}) AS last, countIf({end}) AS matched,\n\
                 arraySlice(arraySort(x -> (x.2, x.1), groupArrayIf((span_id, start_ns, duration_ns, service, {}), {end})), 1, {spss}) AS spans\n\
                 FROM {}\nGROUP BY trace_id",
                src.scope,
                src.col("proj"),
                src.from
            );
        }
        let groups = self.groups(last, &src, &alive, top, spss);
        if top {
            format!(
                "SELECT trace_id, max(g_last) AS last, groupUniqArrayArray(g_b) AS keys\n\
                 FROM ({groups})\nGROUP BY trace_id\nHAVING countIf(alive) > 0{in_slice}\n{order}"
            )
        } else {
            format!(
                "SELECT trace_id, max(g_last) AS last, sumIf(n_end, alive) AS matched,\n\
                 arraySlice(arraySort(x -> (x.2, x.1), arrayFlatten(groupArrayIf(g_spans, alive))), 1, {spss}) AS spans,\n\
                 arrayMap(x -> (x.2, x.3, x.4), arraySort(x -> x.1, groupArrayIf((g_order, g_attrs, n_end, g_spans), alive))) AS groups\n\
                 FROM ({groups})\nGROUP BY trace_id"
            )
        }
    }

    /// The keys a segment writes, in order.
    fn seg_keys(seg: &[Stage]) -> Vec<usize> {
        seg.iter()
            .filter_map(|s| match s {
                Stage::By(j) => Some(*j),
                _ => None,
            })
            .collect()
    }

    /// `trace_id` and the first `n` of `keys`.
    fn partition(keys: &[usize], n: usize) -> String {
        let mut p = String::from("trace_id");
        for j in &keys[..n] {
            p.push_str(&format!(", k{j}, t{j}"));
        }
        p
    }

    /// The segment's fine groups with every aggregate merged over its
    /// prefix, and the condition a group survives by: `(relation, the
    /// aggregates' (index, text, type), the condition)`. The fine level
    /// adds `extra`, the merged level `extra_merged`.
    fn merged(
        &self,
        seg: &[Stage],
        src: &Src,
        alive: &str,
        extra: &[String],
        extra_merged: &[String],
    ) -> (String, Vec<(usize, String, String)>, String) {
        let keys = Self::seg_keys(seg);
        let mut cond = alive.to_string();
        let mut written = 0usize;
        let mut partials: Vec<String> = Vec::new();
        let mut merges: Vec<String> = Vec::new();
        let mut passes: Vec<String> = Vec::new();
        let mut values: Vec<(usize, String, String)> = Vec::new();
        for s in seg {
            match s {
                Stage::Filter(i) => cond = format!("{cond} AND {}", src.col(&format!("q{i}"))),
                Stage::By(_) => written += 1,
                Stage::Agg(i) => {
                    let a = &self.aggs[*i];
                    let direct = written == keys.len();
                    let p = Self::partition(&keys, written);
                    // `name`'s merge over the prefix: the fine group's own
                    // value when the prefix is every key.
                    let m = |merge: &str, name: &str| -> String {
                        if direct {
                            format!("{name} AS m{name}")
                        } else {
                            format!(
                                "{} OVER (PARTITION BY {p}) AS m{name}",
                                merge.replace("{}", name)
                            )
                        }
                    };
                    let cc = match &a.arg {
                        AggArg::Attr { .. } => {
                            format!("({cond}) AND isNotNull({})", src.col(&format!("a{i}")))
                        }
                        _ => cond.clone(),
                    };
                    let av = src.col(&format!("a{i}"));
                    let ak = src.col(&format!("ak{i}"));
                    partials.push(format!("countIf({cc}) AS n{i}"));
                    merges.push(m("sum({})", &format!("n{i}")));
                    let nm = format!("mn{i}");
                    use AggregateOp as A;
                    let (scalar, text, ty) = match (&a.arg, a.op) {
                        (AggArg::Count, _) => (
                            format!("toFloat64({nm})"),
                            format!("toString({nm})"),
                            "'Int64'".to_string(),
                        ),
                        (AggArg::Duration, A::Min | A::Max) => {
                            let f = if a.op == A::Min { "minIf" } else { "maxIf" };
                            partials.push(format!("{f}(duration_ns, {cc}) AS x{i}"));
                            merges.push(m(&format!("{f}({{}}, n{i} > 0)"), &format!("x{i}")));
                            (
                                format!("toFloat64(mx{i})"),
                                format!("toString(mx{i})"),
                                "'DurationI'".to_string(),
                            )
                        }
                        (AggArg::Duration, A::Sum | A::Avg) => {
                            partials.push(format!(
                                "groupArrayIf((toFloat64(duration_ns), start_ns, span_id), {cc}) AS l{i}"
                            ));
                            merges.push(m("groupArrayArray({})", &format!("l{i}")));
                            let f = fold_sum(&format!("ml{i}"));
                            if a.op == A::Sum {
                                (
                                    f.clone(),
                                    format!("toString({f})"),
                                    "'DurationF'".to_string(),
                                )
                            } else {
                                partials.push(format!("sumIf(duration_ns, {cc}) AS ds{i}"));
                                merges.push(m("sum({})", &format!("ds{i}")));
                                (
                                    format!("{f} / {nm}"),
                                    format!("toString(intDiv(mds{i}, {nm}))"),
                                    "'DurationI'".to_string(),
                                )
                            }
                        }
                        (AggArg::Attr { .. }, A::Min | A::Max) => {
                            // `max` is the `min` of the negated value, so
                            // one tuple order breaks a tie by the first span.
                            let sign = if a.op == A::Min { "" } else { "-" };
                            partials.push(format!(
                                "minIf(({sign}assumeNotNull({av}), start_ns, span_id, {ak}), {cc}) AS x{i}"
                            ));
                            merges.push(m(&format!("minIf({{}}, n{i} > 0)"), &format!("x{i}")));
                            (
                                format!("{sign}mx{i}.1"),
                                format!("toString({sign}mx{i}.1)"),
                                format!("if(mx{i}.4 = 'Int64', 'AggInt', 'AggDouble')"),
                            )
                        }
                        (AggArg::Attr { .. }, A::Sum | A::Avg) => {
                            partials.push(format!(
                                "groupArrayIf((assumeNotNull({av}), start_ns, span_id), {cc}) AS l{i}"
                            ));
                            merges.push(m("groupArrayArray({})", &format!("l{i}")));
                            let f = fold_sum(&format!("ml{i}"));
                            if a.op == A::Sum {
                                partials
                                    .push(format!("countIf(({cc}) AND {ak} != 'Int64') AS d{i}"));
                                merges.push(m("sum({})", &format!("d{i}")));
                                (
                                    f.clone(),
                                    format!("toString({f})"),
                                    format!("if(md{i} = 0, 'AggInt', 'AggDouble')"),
                                )
                            } else {
                                (
                                    format!("{f} / {nm}"),
                                    format!("toString({f} / {nm})"),
                                    "'AggDouble'".to_string(),
                                )
                            }
                        }
                        (_, A::Count) => unreachable!("count() reads no field"),
                    };
                    passes.push(format!(
                        "{nm} > 0 AND ({scalar}) {} {}",
                        a.sql_op, a.threshold
                    ));
                    values.push((*i, text, ty));
                }
                Stage::Coalesce => {}
            }
        }
        let mut select_keys = String::new();
        let mut group_by = String::from("trace_id");
        for j in &keys {
            select_keys.push_str(&format!(
                ", {}, {}",
                src.named(&format!("k{j}")),
                src.named(&format!("t{j}"))
            ));
            group_by.push_str(&format!(", k{j}, t{j}"));
        }
        let mut fine_cols: Vec<String> = vec![format!("countIf({cond}) AS n_end")];
        fine_cols.extend(partials);
        fine_cols.extend(extra.iter().cloned());
        let fine = format!(
            "{}SELECT trace_id{select_keys}, {}\nFROM {}\nGROUP BY {group_by}",
            src.scope,
            fine_cols.join(", "),
            src.from
        );
        let mut outer: Vec<String> = vec!["*".to_string()];
        outer.extend(merges);
        outer.extend(extra_merged.iter().cloned());
        let window = format!("SELECT {}\nFROM ({fine})", outer.join(", "));
        let mut pass = String::from("n_end > 0");
        for p in passes {
            pass.push_str(&format!(" AND ({p})"));
        }
        (window, values, pass)
    }

    /// A segment followed by `coalesce()`: its fine groups, merged, and
    /// every span row handed back by name, `v` true where it survives.
    fn explode(&self, seg: &[Stage], src: &Src, alive: &str, top: bool) -> String {
        let mut carried: Vec<String> = vec![
            "span_id".into(),
            "start_ns".into(),
            "duration_ns".into(),
            "s0".into(),
        ];
        if !top {
            carried.push("service".into());
            carried.push("proj".into());
        }
        carried.extend(self.column_names());
        let mut cond = alive.to_string();
        for s in seg {
            if let Stage::Filter(i) = s {
                cond = format!("{cond} AND {}", src.col(&format!("q{i}")));
            }
        }
        let resolved: Vec<String> = carried.iter().map(|c| src.col(c)).collect();
        let tuple = format!("groupArray(tuple({}, {cond})) AS r", resolved.join(", "));
        let (window, _, pass) = self.merged(seg, src, alive, &[tuple], &[]);
        let n = carried.len();
        let mut cols: Vec<String> = carried
            .iter()
            .enumerate()
            .map(|(i, c)| format!("x.{} AS {c}", i + 1))
            .collect();
        cols.push(format!("x.{} AND ga AS v", n + 1));
        format!(
            "SELECT trace_id, {}\nFROM (SELECT trace_id, r, {pass} AS ga\nFROM ({window}))\nARRAY JOIN r AS x",
            cols.join(", ")
        )
    }

    /// The last segment's fine groups: `trace_id`, `g_last`, `alive`, and
    /// `g_b` in the top-K; `n_end`, `g_order`, `g_attrs` and `g_spans` in
    /// the detail.
    fn groups(&self, seg: &[Stage], src: &Src, alive: &str, top: bool, spss: u32) -> String {
        let keys = Self::seg_keys(seg);
        let mut extra: Vec<String> = vec![format!("maxIf(start_ns, {}) AS g_last", src.col("s0"))];
        let mut extra_merged: Vec<String> = Vec::new();
        let mut cond = alive.to_string();
        let mut written = 0usize;
        let mut order: Vec<String> = Vec::new();
        for s in seg {
            match s {
                Stage::Filter(i) => cond = format!("{cond} AND {}", src.col(&format!("q{i}"))),
                Stage::By(_) if !top => {
                    let b = written;
                    written += 1;
                    extra.push(format!("countIf({cond}) AS nb{b}"));
                    extra.push(format!("minIf((start_ns, span_id), {cond}) AS fp{b}"));
                    if written == keys.len() {
                        extra_merged.push(format!("fp{b} AS o{b}"));
                    } else {
                        extra_merged.push(format!(
                            "minIf(fp{b}, nb{b} > 0) OVER (PARTITION BY {}) AS o{b}",
                            Self::partition(&keys, written)
                        ));
                    }
                    order.push(format!("o{b}"));
                }
                _ => {}
            }
        }
        if top {
            extra.push(format!("groupUniqArray({BUCKET}) AS g_b"));
        } else {
            extra.push(format!(
                "arraySlice(arraySort(x -> (x.2, x.1), groupArrayIf((span_id, start_ns, duration_ns, service, {}), {cond})), 1, {spss}) AS g_spans",
                src.col("proj")
            ));
        }
        let (window, values, pass) = self.merged(seg, src, alive, &extra, &extra_merged);
        if top {
            return format!("SELECT trace_id, g_last, g_b, {pass} AS alive\nFROM ({window})");
        }
        let mut attrs: Vec<String> = Vec::new();
        for s in seg {
            match s {
                Stage::By(j) => attrs.push(format!("(k{j}, t{j})")),
                Stage::Agg(i) => {
                    let (_, text, ty) = values
                        .iter()
                        .find(|(x, _, _)| x == i)
                        .expect("every aggregate has a value");
                    attrs.push(format!("({text}, {ty})"));
                }
                _ => {}
            }
        }
        let g_order = if order.is_empty() {
            "0".to_string()
        } else {
            format!("tuple({})", order.join(", "))
        };
        format!(
            "SELECT trace_id, g_last, n_end, g_spans, {g_order} AS g_order, [{}] AS g_attrs, {pass} AS alive\nFROM ({window})",
            attrs.join(", ")
        )
    }
}
