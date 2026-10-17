//! Pure per-stage SQL string builders — the snapshot-testing surface
//! (`tests/sql_snapshots.rs`). Every function here is `AST-derived data →
//! String`: no `ChClient`, no I/O, no randomness. Callers (mainly
//! [`super::plan`]) are responsible for pre-escaping every user-controlled
//! fragment via [`super::escape`] before it reaches these builders — that
//! is the injection boundary, not this module.
//!
//! **Issue #286 made most of that responsibility a type, not a convention.**
//! Boolean predicate fragments arrive as [`super::predicate::CheckedFragment`],
//! string literals as [`super::predicate::CheckedLiteral`], month partition
//! literals as [`super::predicate::MonthLiteral`], and a metric read's
//! bucket/aggregate columns as a [`MetricShape`] — none of which can be built
//! from unchecked text outside `logql::predicate`. The table-name parameters
//! deliberately stay `&str`; `predicate.rs`'s module doc records that residual
//! and why neither candidate mechanism was taken.

use pulsus_model::{FpLiteral, Tenant};

use crate::metrics::TenantSql;

use super::params::Direction;
use super::predicate::{CheckedFragment, CheckedLiteral, MonthLiteral};

/// A half-open-below/closed-above nanosecond time bound (`ts > start AND ts
/// <= end`, docs/schemas.md §3.2), grouped into one parameter so the stage
/// 3/metric builders below stay under clippy's argument-count lint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeWindow {
    pub start_ns: i64,
    pub end_ns: i64,
}

/// How a METRIC scan's lower time bound compares (issue #227 review
/// round 11). `Exclusive` is the ordinary half-open window predicate
/// (`timestamp_ns > start_ns` — the reference's `(t-range, t]`).
/// `Inclusive` (`timestamp_ns >= start_ns`) exists for exactly one case:
/// the planner's `start - range` scan widening UNDERFLOWED i64, so the
/// LOGICAL lower bound sits strictly below the representable timestamp
/// domain and the saturated `i64::MIN` carried in
/// [`TimeWindow::start_ns`] is a VACUOUS bound, not an exclusive one — a
/// sample stored at exactly `i64::MIN` is inside the reference's window,
/// and `>` would silently drop it. A legitimately-computed
/// (non-underflowing) `i64::MIN` bound stays `Exclusive` and keeps
/// excluding a sample at exactly that timestamp. Only
/// [`super::plan::metric_plan`]'s widening decides this; the log-path
/// stage-3 builders take client bounds verbatim (never widened) and stay
/// structurally exclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanLowerBound {
    Exclusive,
    Inclusive,
}

impl ScanLowerBound {
    /// The SQL comparison operator the lower bound renders as. Both forms
    /// are plain range predicates on the `ORDER BY` time column, so
    /// primary-key pruning is identical (`>=` at `i64::MIN` trivially
    /// covers every part, exactly like the saturated `>` did).
    fn sql_op(self) -> &'static str {
        match self {
            ScanLowerBound::Exclusive => ">",
            ScanLowerBound::Inclusive => ">=",
        }
    }
}

/// Declares [`MetricShape`], its COMPLETE variant list and both column
/// renderings from one source — the `vector_agg_ops!` precedent
/// (`crates/pulsus-logql/src/ast.rs:2460-2504`, issue #406), for the same
/// reason it gives: a hand-maintained `ALL` beside a hand-written enum is two
/// sources, and [`MetricShape::from_columns`] enumerates through `ALL`.
///
/// A fifth shape cannot be added without supplying both column literals and
/// landing in `ALL`, so the round-trip test covers it without being edited.
/// There is **no `_ =>` arm** in any [`MetricShape`] impl and no second
/// invocation is possible (`E0428`).
///
/// Per-variant docs travel through `$(#[$meta:meta])*`.
macro_rules! metric_shapes {
    ($($(#[$meta:meta])* $variant:ident => ($bucket:literal, $agg:literal),)+) => {
        /// The complete set of `(bucket_col, agg_expr)` column shapes a
        /// metric read can have: rollup vs raw × bytes vs count. Sealed so
        /// [`MetricSource`] cannot carry caller text (issue #286).
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum MetricShape { $($(#[$meta])* $variant),+ }

        impl MetricShape {
            /// Every variant, in declaration order — emitted by the same
            /// invocation that declares them, so nothing that enumerates
            /// through this slice can miss a shape the enum has.
            pub const ALL: &'static [MetricShape] = &[$(MetricShape::$variant),+];

            /// Wildcard-free by construction: the macro emits one arm per
            /// variant and there is no `_ =>` arm to add one to.
            pub const fn bucket_col(self) -> &'static str {
                match self { $(MetricShape::$variant => $bucket),+ }
            }

            /// Wildcard-free by construction, as [`MetricShape::bucket_col`].
            pub const fn agg_expr(self) -> &'static str {
                match self { $(MetricShape::$variant => $agg),+ }
            }
        }
    };
}

metric_shapes! {
    /// `log_metrics_<res>` serving a count-family reducer.
    RollupCount => ("bucket_ns", "sum(count)"),
    /// `log_metrics_<res>` serving a bytes-family reducer.
    RollupBytes => ("bucket_ns", "sum(bytes)"),
    /// The `log_samples` raw fallback, count family.
    RawCount => ("timestamp_ns", "count()"),
    /// The `log_samples` raw fallback, bytes family.
    RawBytes => ("timestamp_ns", "sum(length(body))"),
}

impl MetricShape {
    /// The rollup shape for a count/bytes reducer.
    pub const fn rollup(is_bytes: bool) -> Self {
        if is_bytes {
            MetricShape::RollupBytes
        } else {
            MetricShape::RollupCount
        }
    }

    /// The raw-fallback shape for a count/bytes reducer.
    pub const fn raw(is_bytes: bool) -> Self {
        if is_bytes {
            MetricShape::RawBytes
        } else {
            MetricShape::RawCount
        }
    }

    /// The inverse, derived from [`MetricShape::ALL`] — so a shape added to
    /// the `metric_shapes!` invocation is covered here without this function
    /// being edited, and no `_ =>` arm exists that could swallow one.
    /// `None` for any other column pair.
    pub fn from_columns(bucket_col: &str, agg_expr: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|s| s.bucket_col() == bucket_col && s.agg_expr() == agg_expr)
    }
}

/// What a metric statement aggregates (issue #507).
///
/// **A sibling of [`MetricShape`], not a fifth variant of it.** A fifth
/// variant would make [`MetricShape::from_columns`] — and therefore
/// `exec.rs`'s `metric_source` panic — reachable for a pair it cannot
/// serve, because the unwrapped form has no fixed `agg_expr` at all.
///
/// **Every arm carries CHECKED VALUES, never statement text.** A field
/// holding rendered SQL would walk around `predicate.rs`'s seal — the
/// property that SQL text is minted in one module — and turn it into a
/// convention.
#[derive(Debug, Clone, PartialEq)]
pub enum MetricValue {
    /// `count()` / `sum(length(body))` over the sealed column pairs —
    /// the counting plans.
    Shaped(MetricShape),
    /// A counting reducer, or `absent_over_time`, whose pipeline holds only
    /// stages that read labels (issue #624, part 2): part 1's statements
    /// count the rows, and the stages run once per returned group. Carries
    /// the column shape and the aggregation today's route runs, which the
    /// fold and the fallbacks need.
    Staged(Box<StagedCount>),
    /// One of the eleven `| unwrap` reducers over a pipeline with no
    /// parser (issue #624, part 4) or with one (part 4b): the statement
    /// reduces each window, and the stages run once per returned key.
    Reduced(Box<ReducedUnwrap>),
}

impl MetricValue {
    /// Whether this lowered plan answers an instant query at its one grid
    /// point (issue #624, part 5).
    pub fn instant(&self) -> bool {
        match self {
            MetricValue::Staged(s) => s.instant,
            MetricValue::Reduced(r) => r.instant,
            MetricValue::Shaped(_) => false,
        }
    }
}

/// The conversion of a lowered `| unwrap` (issue #624, part 4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnwrapConversion {
    /// `| unwrap x`.
    Number,
    /// `| unwrap duration(x)` and `| unwrap duration_seconds(x)`.
    Duration,
    /// `| unwrap bytes(x)`.
    Bytes,
}

/// How a lowered `| unwrap` reducer keys its series (issue #624, part 4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyMode {
    /// `by (L)`: the key is a class of stream labels on `L` and the
    /// metadata values of `L`.
    Keep(Vec<String>),
    /// No grouping, or `without (L)`: every label but the names in `E`, the
    /// unwrapped label among them.
    Drop(Vec<String>),
}

/// [`MetricValue::Reduced`]'s payload (issue #624, part 4).
#[derive(Debug, Clone, PartialEq)]
pub struct ReducedUnwrap {
    /// One of the eleven `| unwrap` reducers.
    pub reducer: pulsus_logql::RangeAggOp,
    /// `quantile_over_time`'s parameter.
    pub quantile: Option<f64>,
    /// The `| unwrap` label, read from structured metadata.
    pub label: String,
    pub conversion: UnwrapConversion,
    pub mode: KeyMode,
    /// The stages each key runs: the pipeline less its line filters and
    /// the label filters the statement applied.
    pub key_stages: Vec<pulsus_logql::Stage>,
    /// The client aggregation today's route runs for this query.
    pub todays_route: super::plan::ClientAgg,
    /// The plan answers an instant query at its one grid point (part 5).
    pub instant: bool,
    /// The one parser before the `| unwrap`, which the statement runs
    /// (issue #624, part 4b); `None`: `x` is read from structured metadata.
    pub parser: Option<ReducedParser>,
}

/// The parser of a lowered `| unwrap` (issue #624, part 4b): what the
/// statement's key column `pk` holds, and where it reads `x`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReducedParser {
    /// `| json`, bare or targeted: `keys` the labels `pk` holds, each with
    /// its top-level source; `x_path` the field segments to `x`.
    Json {
        form: UnwrapForm,
        keys: Vec<UnwrapKeyLabel>,
        x_path: Vec<String>,
    },
    /// `| logfmt`: `names` the sanitised keys whose fields `pk` holds;
    /// `x_source` the field key `x` is read from; `x_destination` the
    /// targeted form's destination for it, `None` on the bare form.
    Logfmt {
        names: Vec<String>,
        x_source: String,
        x_destination: Option<String>,
    },
    /// `| regexp` or `| pattern`: `groups` the named groups `pk` holds,
    /// `(name, 1-based index)`; `x_index` the index of `x`'s group.
    Capture {
        parser: CaptureParser,
        pattern: String,
        groups: Vec<(String, usize)>,
        x_index: usize,
    },
}

/// What a lowered `| unwrap` statement reads of the resolved streams
/// (issue #624, part 4).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReducedKeyColumns {
    /// `None`: each fingerprint is its own class. Otherwise the
    /// fingerprints of each class, by class id.
    pub classes: Option<Vec<Vec<FpLiteral>>>,
    /// Each selected stream's hash, when a class holds two or more
    /// fingerprints; otherwise empty.
    pub hashes: Vec<(FpLiteral, u64)>,
    /// Each metadata name the key reads or removes, with the fingerprints
    /// of the streams carrying it as a stream label.
    pub blank: Vec<(String, Vec<FpLiteral>)>,
    /// The selected streams labelled `x`, each with its value: the
    /// statement reads `x` from the stream label on these (issue #624,
    /// part 4b, D3).
    pub x_values: Vec<(FpLiteral, String)>,
    /// Each name `pk` holds, with the fingerprints whose streams carry it
    /// as a stream label: the parser's value is left out of `pk` there
    /// (keep mode; issue #624, part 4b).
    pub pk_blank: Vec<(String, Vec<FpLiteral>)>,
}

/// [`MetricValue::Staged`]'s payload (issue #624, part 2).
#[derive(Debug, Clone, PartialEq)]
pub struct StagedCount {
    /// The statement's column shape: `count()` or `sum(length(body))`.
    pub shape: MetricShape,
    /// The client aggregation today's route runs for this query: the full
    /// pipeline, the reducer and `absent_over_time`'s labels.
    pub todays_route: super::plan::ClientAgg,
    /// The pipeline's one `regexp` stage, when the statement runs it (issue
    /// #624, part 3a): its captures join the group key.
    pub regexp: Option<RegexpCount>,
    /// The pipeline's one `| json` stage, when the statement reads its key
    /// labels (issue #624, part 3b): the keys join the group key.
    pub json: Option<JsonCount>,
    /// The pipeline's one `| logfmt` stage, when the statement sends its
    /// relevant fields (issue #624, part 3d): they join the group key.
    pub logfmt: Option<LogfmtCount>,
    /// The plan answers an instant query at its one grid point (issue
    /// #624, part 5).
    pub instant: bool,
}

/// The `| logfmt` stage of a lowered count (issue #624, part 3d): the
/// sanitised key names whose fields the stage can read for the answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogfmtCount {
    pub names: Vec<String>,
}

/// The line grammar a lowered `| logfmt` count decides (issue #624, part
/// 3d): separators are the bytes at or below `' '`; a key is one or more
/// printable ASCII bytes other than `"` and `=`; a value is unquoted (no
/// separator, `=` or `"`) or quoted, its escapes the ones both decoders
/// unquote; a separator or the end follows every field. A line matching it
/// is one the reference's decoder reads with no error, no field directly
/// followed by a byte above `' '`, and every key printable ASCII.
pub const LOGFMT_DECIDED_LINE: &str = r#"^[\x00-\x20]*(?:[!#-<>-~]+(?:=(?:"(?:[^"\\]|\\["\\/'bfnrt]|\\u[0-9A-Fa-f]{4})*"|[^\x00-\x20="]*))?(?:[\x00-\x20]+|$))*$"#;

/// One field of a decided line (issue #624, part 3d), as
/// [`LOGFMT_DECIDED_LINE`] spells it.
pub const LOGFMT_FIELD: &str = r#"[!#-<>-~]+(?:=(?:"(?:[^"\\]|\\.)*"|[^\x00-\x20="]*))?"#;

/// The bytes a decided key may hold that the key sanitiser turns into `_`:
/// every printable ASCII byte but a letter, a digit, `_`, `"` and `=`.
pub const LOGFMT_KEY_PUNCTUATION: &str = "!#$%&'()*+,-./:;<>?@[\\]^`{|}~";

/// The two columns of a lowered `| logfmt` count (issue #624, part 3d):
/// `decided`, the line matching [`LOGFMT_DECIDED_LINE`], and `fields`, the
/// line's fields whose sanitised key is one of `names`, in line order,
/// joined by one space.
pub fn logfmt_count_columns(names: &[String]) -> [String; 2] {
    use super::predicate::literal;
    let names: Vec<String> = names
        .iter()
        .map(|n| literal(n).as_sql().to_string())
        .collect();
    [
        format!(
            "toUInt8(match(body, {})) AS decided",
            literal(LOGFMT_DECIDED_LINE).as_sql()
        ),
        format!(
            "arrayStringConcat(arrayFilter(f -> has([{}], concat(if(substring(f, 1, 1) BETWEEN '0' AND '9', '_', ''), \
             translate(substringIndex(f, '=', 1), {}, {}))), extractAll(body, {})), ' ') AS fields",
            names.join(", "),
            literal(LOGFMT_KEY_PUNCTUATION).as_sql(),
            literal(&"_".repeat(LOGFMT_KEY_PUNCTUATION.len())).as_sql(),
            literal(LOGFMT_FIELD).as_sql()
        ),
    ]
}

/// The `| json` stage of a lowered count (issue #624, part 3b): its form
/// and the key labels the statement reads, `(label, top-level source)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonCount {
    pub form: UnwrapForm,
    pub keys: Vec<UnwrapKeyLabel>,
}

/// The reader columns of a lowered `| json` count (issue #624, part 3b),
/// each `<expr> AS <name>`, ending in `decided` and `keys`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonCountColumns {
    pub readers: Vec<String>,
}

/// What a lowered count extracts from the stored line into its group key
/// (issue #624, parts 3a and 3b).
#[derive(Debug, Clone, Copy)]
pub enum GroupExtraction<'a> {
    Regexp(&'a super::predicate::RegexpGroupColumns),
    Json(&'a JsonCountColumns),
    Logfmt(&'a [String; 2]),
}

impl GroupExtraction<'_> {
    fn key_columns(&self) -> &'static str {
        match self {
            GroupExtraction::Regexp(_) => "matched, caps",
            GroupExtraction::Json(_) => "keys",
            GroupExtraction::Logfmt(_) => "fields",
        }
    }
    fn lines(&self, indent: &str) -> Vec<String> {
        match self {
            GroupExtraction::Regexp(r) => regexp_extraction_lines(r, indent).to_vec(),
            GroupExtraction::Json(j) => {
                let n = j.readers.len();
                j.readers
                    .iter()
                    .enumerate()
                    .map(|(i, r)| format!("{indent}{r}{}", if i + 1 < n { "," } else { "" }))
                    .collect()
            }
            GroupExtraction::Logfmt([decided, fields]) => {
                vec![format!("{indent}{decided},"), format!("{indent}{fields}")]
            }
        }
    }
    /// The filter under the extraction level: a `| json` or `| logfmt`
    /// count fails with code 395 on the first row the database did not
    /// decide.
    fn throw_line(&self, indent: &str) -> Option<String> {
        match self {
            GroupExtraction::Regexp(_) => None,
            GroupExtraction::Json(_) | GroupExtraction::Logfmt(_) => {
                Some(format!("{indent}WHERE throwIf(decided = 0) = 0"))
            }
        }
    }
}

/// [`JsonCountColumns`] for a lowered `| json` count (issue #624, part 3b):
/// per key label #507's readers (`l<i>_r`, `_amb`, `_str`, `_int`,
/// `_absent`), the nesting bound, and on the bare form the key budget and
/// `doc_ok` (a valid document whose top level is an object); `decided` is
/// their product and `keys` the decided texts.
pub fn json_count_columns(
    form: UnwrapForm,
    keys: &[UnwrapKeyLabel],
) -> Result<JsonCountColumns, KeyStatementRefusal> {
    use super::predicate::{ReaderColumns, json_depth_bound, json_flatten_key_budget_bound};
    let mut w: Vec<String> = Vec::new();
    let mut decided: Vec<String> = Vec::new();
    for (i, key) in keys.iter().enumerate() {
        let t = format!("l{i}");
        w.extend(json_value_reader(
            ReaderColumns::Key(i),
            std::slice::from_ref(&key.source),
            form,
        )?);
        decided.push(format!("toUInt8(({t}_str + {t}_int + {t}_absent) > 0)"));
    }
    w.push(format!(
        "toUInt8({}) AS depth_ok",
        json_depth_bound().as_sql()
    ));
    decided.push("depth_ok".to_string());
    if form == UnwrapForm::Bare {
        w.push(format!(
            "toUInt8({}) AS keybudget_ok",
            json_flatten_key_budget_bound().as_sql()
        ));
        w.push("toUInt8(isValidJSON(body) AND JSONType(body) = 'Object') AS doc_ok".to_string());
        decided.push("keybudget_ok".to_string());
        decided.push("doc_ok".to_string());
    }
    w.push(format!("toUInt8({}) AS decided", decided.join(" * ")));
    if keys.is_empty() {
        w.push("CAST([], 'Array(Tuple(UInt8, String))') AS keys".to_string());
    } else {
        let items: Vec<String> = (0..keys.len())
            .map(|i| {
                let t = format!("l{i}");
                format!(
                    "(toUInt8(decided * ({t}_str + {t}_int)), if(decided = 1, if({t}_str = 1, \
                     substring({t}_r, 2, length({t}_r) - 2), if({t}_int = 1, {t}_r, '')), ''))"
                )
            })
            .collect();
        w.push(format!("[{}] AS keys", items.join(", ")));
    }
    Ok(JsonCountColumns { readers: w })
}

/// #507's readers of one JSON value at `path` (issue #624, parts 3b and
/// 4b), each `<expr> AS <prefix>_<name>`: `_r` the raw text, `_amb` a
/// spelling `JSONExtractRaw` does not read (the bare form), `_str` a string
/// with no escape, `_int` an integer our parser renders the same way, and
/// `_absent` no value at all.
fn json_value_reader(
    columns: super::predicate::ReaderColumns,
    path: &[String],
    form: UnwrapForm,
) -> Result<Vec<String>, KeyStatementRefusal> {
    use super::predicate::{
        literal, unwrap_label_integer_text, unwrap_name_absence, unwrap_name_ambiguity,
    };
    let t = columns.prefix();
    let backslash = literal("\\");
    let segments: Vec<String> = path
        .iter()
        .map(|seg| literal(seg).as_sql().to_string())
        .collect();
    let segments = segments.join(", ");
    let name = path.first().map_or("", String::as_str);
    Ok(vec![
        format!("JSONExtractRaw(body, {segments}) AS {t}_r"),
        format!(
            "{} AS {t}_amb",
            match form {
                UnwrapForm::Targeted => "toUInt8(0)".to_string(),
                UnwrapForm::Bare => unwrap_name_ambiguity(name)?.as_sql().to_string(),
            }
        ),
        // Issue #624, part 3d: from 26.8 an integer outside
        // `[-2^63, 2^64)` reads back quoted, as a string; a quoted value of
        // 19 or more digits is left to today's route.
        format!(
            "toUInt8({t}_amb = 0 AND startsWith({t}_r, '\"') AND position({t}_r, {}) = 0 AND NOT match({t}_r, '^\"-?[0-9]{{19,}}\"$')) AS {t}_str",
            backslash.as_sql()
        ),
        format!(
            "toUInt8({t}_amb = 0 AND {} AND JSONType(body, {segments}) IN ('Int64', 'UInt64')) AS {t}_int",
            unwrap_label_integer_text(columns).as_sql()
        ),
        format!(
            "{} AS {t}_absent",
            unwrap_name_absence(columns, name, form)?.as_sql()
        ),
    ])
}

/// The `regexp` stage of a lowered count (issue #624, part 3a, D2 and D4):
/// its pattern as the query wrote it, and the named groups whose captures
/// the statement sends — `(name, 1-based group index)`, in capture-index
/// order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegexpCount {
    pub pattern: String,
    pub groups: Vec<(String, usize)>,
    /// Issue #624, part 3c: the stage is a `| pattern`, whose groups are
    /// its named captures, `(name, 1-based ordinal among them)`.
    pub parser: CaptureParser,
}

/// Which parser a [`RegexpCount`] lowers (issue #624, parts 3a and 3c).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptureParser {
    Regexp,
    Pattern,
}

/// Which `| json` form a lowered statement reads (issues #507 and #624).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnwrapForm {
    /// `| json` with no extractions: the line is flattened, so a label name
    /// can have spellings `JSONExtractRaw` does not read.
    Bare,
    /// `| json <label>="<path>"…`: every path is exact.
    Targeted,
}

/// One key label a lowered `| json` statement reads (issues #507 and #624).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnwrapKeyLabel {
    /// The label name in the answer.
    pub label: String,
    /// The top-level source key; equal to `label` on the bare form.
    pub source: String,
}

/// Why a lowered statement's readers cannot be rendered. Every one sends
/// the query to today's route; none is a client error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyStatementRefusal {
    Grid(super::predicate::BucketGridRefusal),
    Reader(super::predicate::KeyReaderRefusal),
}

impl From<super::predicate::BucketGridRefusal> for KeyStatementRefusal {
    fn from(r: super::predicate::BucketGridRefusal) -> Self {
        KeyStatementRefusal::Grid(r)
    }
}

impl From<super::predicate::KeyReaderRefusal> for KeyStatementRefusal {
    fn from(r: super::predicate::KeyReaderRefusal) -> Self {
        KeyStatementRefusal::Reader(r)
    }
}

/// **The parser the reduced statement runs under** (issue #507, review
/// round 4; kept by issue #624, part 4).
///
/// `toFloat64OrNull` uses a fast approximate parser by default, and it is
/// not correctly rounded. One row is enough for that to be a wrong answer.
/// Measured on 26.3.29.7:
///
/// ```text
///   SELECT hex(reinterpretAsUInt64(assumeNotNull(
///       toFloat64OrNull('9367469347402735e292'))))
///
///   default                             -> 7FE0ACB5CADC2918
///   SETTINGS precise_float_parsing = 1  -> 7FE0ACB5CADC2917
///   "9367469347402735e292".parse::<f64>() ->  7FE0ACB5CADC2917
/// ```
///
/// Under the setting each text issue #507 measured either converts to
/// the same bits as `f64::from_str` or converts to NULL: `"E12"`, `"e3"`,
/// `"."` and `"1.7976931348623159e308"` convert to NULL, `"0e999999"` to
/// `+0`, `"5e-324"` to bits `0x1`, and `"2e-324"` to NULL where Rust gives
/// `0`. A NULL is an undecided text, which our parser reads on today's
/// route.
///
/// **It is rendered into the statement text rather than sent as a
/// connection setting**, so the EXPLAIN payload reports the query that
/// executes. It belongs to [`metric_range_reduced`]: no other builder
/// converts text to a float in SQL.
const UNWRAP_PARSER_SETTING: &str = "precise_float_parsing = 1";

/// Which physical table a metric read targets, and that table's
/// bucket/aggregate column shape — the rollup-vs-raw routing decision
/// [`super::plan::metric_plan`] makes, grouped into one parameter (same
/// clippy argument-count reason as [`TimeWindow`]). Rollup-served reads
/// `log_metrics_<res>` with `bucket_ns`/`sum(count)`|`sum(bytes)`; the raw
/// fallback reads `log_samples` with `timestamp_ns`/`count()`|`sum(length(body))`.
///
/// **Issue #286 — the fields are private and the columns are an enum.**
/// `bucket_col`/`agg_expr` were `pub &'a str` interpolated verbatim into the
/// `SELECT` list and the `WHERE`, so a caller could pass
/// `&format!("match(body, {…})", …)`. They are now a [`MetricShape`], which
/// has exactly four inhabitants. The asymmetry with
/// [`super::plan::MetricPlan`], whose equivalents are still strings, is not a
/// design preference: that struct's field set is pinned by issue #293's
/// frozen `tests/golden/plan_build_differential.txt` — see
/// [`super::plan::MetricPlan::source_shape`].
///
/// **`table` deliberately stays `&str`.** No enforced property covers it;
/// `predicate.rs`'s module doc carries that residual, its ground (an
/// observation about `PlanCtx`, of the kind review round 2 refused for
/// `services`), and the two candidate mechanisms that were measured and
/// declined.
#[derive(Debug, Clone, Copy)]
pub struct MetricSource<'a> {
    table: &'a str,
    shape: MetricShape,
}

impl<'a> MetricSource<'a> {
    /// The ONLY constructor. The column shape can only be one of
    /// [`MetricShape::ALL`]; `table` is a trusted schema name — see the
    /// residual note on the type.
    ///
    /// The pre-#286 bypass — caller text straight into the `SELECT` list —
    /// no longer compiles:
    ///
    /// ```compile_fail,E0560
    /// let _ = pulsus_read::logql::sql::MetricSource {
    ///     table: "t",
    ///     bucket_col: "b",
    ///     agg_expr: "match(body, '(')",
    /// };
    /// ```
    ///
    /// **The code above is `E0560`, not the `E0451` a reader might expect**,
    /// and it was measured rather than assumed: the two column fields no
    /// longer exist, so rustc reports `struct MetricSource<'_> has no field
    /// named bucket_col`/`agg_expr` and aborts before the privacy check on
    /// `table`. The privacy seal on its own is this, naming only real fields:
    ///
    /// ```compile_fail,E0451
    /// use pulsus_read::logql::sql::{MetricShape, MetricSource};
    /// let _ = MetricSource { table: "t", shape: MetricShape::RawCount };
    /// ```
    ///
    /// **Note on what a `compile_fail` fence does and does not check
    /// (measured, issue #286):** rustdoc runs the snippet and requires it to
    /// FAIL, but it does **not** verify the annotated error code —
    /// `compile_fail,E0999`, a code that does not exist, passes. The codes
    /// above are therefore documentation of a measurement, not a gate. What
    /// makes a fence worth something is its REMOVAL TEST, and both of these
    /// have been watched go green when the property they name is deleted:
    /// `pub table` + `pub shape` turns the second one green, and the first
    /// one goes green **only** when the entire pre-#286
    /// `{ pub table, pub bucket_col, pub agg_expr }` shape is restored —
    /// re-adding just the two column fields leaves it red on the missing
    /// `shape`, which was measured rather than assumed (`predicate.rs`'s
    /// module doc carries the whole table). The compiling twin below shares
    /// their skeleton so a typo cannot make them pass for the wrong reason.
    ///
    /// ```
    /// use pulsus_read::logql::sql::{MetricShape, MetricSource};
    /// let s = MetricSource::new("log_samples", MetricShape::RawCount);
    /// assert_eq!(s.shape().agg_expr(), "count()");
    /// ```
    pub fn new(table: &'a str, shape: MetricShape) -> Self {
        MetricSource { table, shape }
    }

    /// The physical table name.
    pub fn table(&self) -> &'a str {
        self.table
    }

    /// The sealed bucket/aggregate column shape.
    pub fn shape(&self) -> MetricShape {
        self.shape
    }
}

/// Stage 1 — single-pass stream resolution over `log_streams_idx`
/// (docs/schemas.md §3.2). `months` are pre-rendered `'YYYY-MM-01'` date
/// literals (at least one); `positive_branches`/`negative_branches` are
/// pre-rendered, already-parenthesized `(key = '...' AND ...)` OR-branches
/// (see [`super::plan::normalize_matchers`]).
///
/// **Pure-positive selectors collapse byte-for-byte to docs/schemas.md
/// §3.2's canonical `HAVING uniqExact(key, val) = n` form** (architect plan
/// amendment §1) — the `negative_branches.is_empty()` branch below is
/// load-bearing for that byte-exact requirement; changing its shape breaks
/// the snapshot contract.
pub fn stage1(
    tenant: &Tenant,
    streams_idx_table: &str,
    months: &[MonthLiteral],
    positive_branches: &[CheckedFragment],
    negative_branches: &[CheckedFragment],
) -> String {
    let month_clause = month_clause(months);

    let mut where_branches: Vec<&str> = positive_branches
        .iter()
        .map(CheckedFragment::as_sql)
        .collect();
    where_branches.extend(negative_branches.iter().map(CheckedFragment::as_sql));
    let where_or_list = where_branches.join(" OR ");

    let having = if negative_branches.is_empty() {
        format!("uniqExact(key, val) = {}", positive_branches.len())
    } else {
        let pos_or = join_fragments(positive_branches);
        let neg_or = join_fragments(negative_branches);
        format!(
            "uniqExactIf((key, val), {pos_or}) = {}\n   AND countIf({neg_or}) = 0",
            positive_branches.len()
        )
    };

    format!(
        "SELECT fingerprint\nFROM {streams_idx_table}\nWHERE org_id = {} AND {month_clause}\n  AND ({where_or_list})\nGROUP BY fingerprint\nHAVING {having}",
        tenant.sql_literal()
    )
}

/// A `count()` selectivity probe over one matcher key's index prefix
/// (docs/schemas.md §3.2: "the planner orders matchers by selectivity
/// (cheap `count()` probes on index prefixes)"). Only computed when the
/// selector contains at least one regex matcher — pure-equality selectors
/// are point ranges and skip probes entirely (architect plan: "Selectivity
/// probes").
pub fn probe(
    tenant: &Tenant,
    streams_idx_table: &str,
    months: &[MonthLiteral],
    key_literal: &CheckedLiteral,
) -> String {
    let month_clause = month_clause(months);
    let key_literal = key_literal.as_sql();
    format!(
        "SELECT count() AS n\nFROM {streams_idx_table}\nWHERE org_id = {} AND {month_clause} AND key = {key_literal}",
        tenant.sql_literal()
    )
}

/// Labels discovery (#13 `GET|POST /api/logs/v1/labels`): every distinct
/// `log_streams_idx` key of a stream ACTIVE within `window`, ascending.
/// Budget-capped like every other index scan in this module
/// (`LogQlEngine::budget_settings`).
///
/// **Issue #399:** the month partition alone is not the requested window —
/// `log_streams_idx` carries no time column at all (`month Date, key, val,
/// fingerprint`), so the window arrives as the active-fingerprint set the
/// caller resolved from the log rollup. The month predicate is kept exactly
/// where it was: it is the partition-pruning bound, and the set narrows within
/// it.
///
/// **Issue #603: `fingerprints` is a literal list, never a subquery.** The
/// caller runs [`active_fingerprints`] as its own statement and renders its
/// result here, which is what `/series` already did — the predicate, and so
/// the answer, is the one the nested form expressed. It is never empty: the
/// caller returns its empty result before building any statement
/// (`super::exec::LogQlEngine::run_discovery`), and its length is bounded by
/// `EngineConfig::max_streams` before it is rendered.
pub fn label_names(
    tenant: &Tenant,
    streams_idx_table: &str,
    months: &[MonthLiteral],
    fingerprints: &[FpLiteral],
) -> String {
    format!(
        "SELECT DISTINCT key AS name\nFROM {streams_idx_table}\nWHERE org_id = {} AND {}\n  AND fingerprint IN ({})\nORDER BY name",
        tenant.sql_literal(),
        month_clause(months),
        fp_list(fingerprints)
    )
}

/// Label-values discovery (#13 `GET /api/logs/v1/label/{{name}}/values`):
/// every distinct value of one key, over the streams ACTIVE within
/// `window`, ascending. `key_literal` is a pre-escaped ClickHouse string
/// literal (see [`super::escape::ch_string`]).
///
/// **Issue #399:** same shape and same reason as [`label_names`] — the month
/// predicate prunes partitions, the active-fingerprint set applies the
/// request's own window. **Issue #603:** that set is a literal list with
/// exactly the precondition [`label_names`] documents. The key predicate keeps
/// its place in the primary-key prefix.
pub fn label_values(
    tenant: &Tenant,
    streams_idx_table: &str,
    months: &[MonthLiteral],
    key_literal: &CheckedLiteral,
    fingerprints: &[FpLiteral],
) -> String {
    let key_literal = key_literal.as_sql();
    format!(
        "SELECT DISTINCT val AS value\nFROM {streams_idx_table}\nWHERE org_id = {} AND {} AND key = {key_literal}\n  AND fingerprint IN ({})\nORDER BY value",
        tenant.sql_literal(),
        month_clause(months),
        fp_list(fingerprints)
    )
}

/// Detected-labels aggregation over the stream index (issue #170,
/// docs/api.md §2.6): one output row per distinct key within `months`,
/// with `uniqExact(val)` as the exact cardinality (a REGISTERED
/// divergence, not an unrecorded improvement: the reference reports a p14
/// hyperloglog estimate — `detected-cardinality-exact-not-estimated` in
/// docs/benchmarks/logs-differential-ledger.md, which carries the
/// per-family measurements and the cost that decided it) and
/// `non_id_values` counting
/// values that are neither a float (`toFloat64OrNull`) nor a UUID
/// ([`super::predicate::non_id_values_expr`], whose `UUID_RE` constant moved
/// there with it in issue #286) — the server-side half of the reference's
/// `containsAllIDTypes` relevance filter (the keep rule — static label OR
/// `non_id_values > 0` — applies client-side in `exec`). `fingerprints` is the
/// active-fingerprint set as a literal list, with the precondition
/// [`label_names`] documents. **Never touches `log_samples`** — it reads the
/// stream index alone, month-partition-pruned, server-side aggregated (fan-in
/// is one row per key, never per value).
///
/// **Issue #399:** before this, the only time bound was `month`, so a
/// ten-minute request was answered from the whole calendar month.
pub fn detected_labels(
    tenant: &Tenant,
    streams_idx_table: &str,
    months: &[MonthLiteral],
    fingerprints: &[FpLiteral],
) -> String {
    let month_clause = month_clause(months);
    let non_id_values = super::predicate::non_id_values_expr();
    let non_id_values = non_id_values.as_sql();
    let active = fp_list(fingerprints);
    format!(
        "SELECT key, uniqExact(val) AS cardinality, {non_id_values} AS non_id_values\nFROM {streams_idx_table}\nWHERE org_id = {} AND {month_clause}\n  AND fingerprint IN ({active})\nGROUP BY key\nORDER BY key",
        tenant.sql_literal()
    )
}

/// The rollup bucket CONTAINING `start_ns` — the activity semi-join's
/// lower bound (issue #399).
///
/// The rollup MV stores `bucket_ns = intDiv(timestamp_ns, res) * res`
/// (`crates/pulsus-schema/src/catalog.rs`, `log_metrics_{{…}}_mv`), so a
/// sample inside the request's `(start, end]` window can sit in a bucket
/// that starts at or before `start`; comparing `bucket_ns` against
/// `start_ns` itself — the half-open shape [`log_stats_rollup`] uses on
/// the SAMPLE axis, where it is correct — would silently drop that
/// stream. Flooring makes the filter conservative in the only safe
/// direction: it can over-include by at most one bucket (5s by default)
/// at each edge and can never drop a stream with a line in the window.
///
/// Floor division (`div_euclid`), not truncation, so a negative
/// `start_ns` rounds DOWN; `saturating_mul` so no input can overflow.
pub fn activity_lower_bucket_ns(start_ns: i64, rollup_res_ns: u64) -> i64 {
    let res = i64::try_from(rollup_res_ns).unwrap_or(i64::MAX).max(1);
    start_ns.div_euclid(res).saturating_mul(res)
}

/// Fingerprints with at least one log line in (approximately) `window` —
/// the stream-activity semi-join source shared by [`detected_labels`],
/// [`label_names`], [`label_values`] and `/series` (issue #399).
///
/// `log_streams_idx` has no time column, so the window has to come from the
/// one table that records per-stream activity in time: the log rollup
/// `log_metrics_<res>` (`ORDER BY (fingerprint, bucket_ns)`, `PARTITION BY
/// toDate(fromUnixTimestamp64Nano(bucket_ns))`), already the source for
/// `/stats` and `/volume`.
///
/// **Issue #603: this renders a statement of its own on every path.** It is no
/// longer embedded as a subquery anywhere — the four endpoints run it, cap its
/// result and render that result as a literal list. `fingerprints` = `Some`
/// still pushes the caller's stage-1 result into this scan so it reads PK
/// point ranges; unscoped, `fingerprint` is unconstrained and only the
/// `bucket_ns` range and the daily partitions bound it.
///
/// `DISTINCT` on the PK prefix is a streaming distinct and is load-bearing:
/// without it the result carries one row per bucket per stream.
///
/// The lower bound is [`activity_lower_bucket_ns`], NOT `start_ns` — see
/// that function for why the obvious `bucket_ns > start_ns` is wrong here.
pub fn active_fingerprints(
    tenant: &Tenant,
    rollup_table: &str,
    fingerprints: Option<&[FpLiteral]>,
    window: TimeWindow,
    rollup_res_ns: u64,
) -> String {
    let TimeWindow { start_ns, end_ns } = window;
    let lower = activity_lower_bucket_ns(start_ns, rollup_res_ns);
    let mut sql = format!(
        "SELECT DISTINCT fingerprint FROM {rollup_table} WHERE org_id = {} AND ",
        tenant.sql_literal()
    );
    if let Some(fps) = fingerprints {
        sql.push_str(&format!("fingerprint IN ({}) AND ", fp_list(fps)));
    }
    sql.push_str(&format!("bucket_ns >= {lower} AND bucket_ns <= {end_ns}"));
    sql
}

/// The `month = '...'` / `month IN (...)` clause shared by every stage-1-
/// style `log_streams_idx` scan in this module (`months` is at least one
/// pre-rendered `'YYYY-MM-01'` date literal).
fn month_clause(months: &[MonthLiteral]) -> String {
    if months.len() == 1 {
        format!("month = {}", months[0].as_sql())
    } else {
        format!(
            "month IN ({})",
            months
                .iter()
                .map(MonthLiteral::as_sql)
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

/// `" OR "`-joins a fragment list — the one place [`stage1`]'s `HAVING`
/// conditional form unwraps its branches.
fn join_fragments(branches: &[CheckedFragment]) -> String {
    branches
        .iter()
        .map(CheckedFragment::as_sql)
        .collect::<Vec<_>>()
        .join(" OR ")
}

/// Stage 2 — hydration (docs/schemas.md §3.2 line 307), byte-exact to the
/// canonical shape: `SELECT fingerprint, service, labels FROM log_streams
/// WHERE fingerprint IN (...)`.
pub fn stage2(tenant: &Tenant, streams_table: &str, fingerprints: &[FpLiteral]) -> String {
    let fp_list = fp_list(fingerprints);
    format!(
        "SELECT fingerprint, service, labels FROM {streams_table} WHERE org_id = {} AND fingerprint IN ({fp_list})",
        tenant.sql_literal()
    )
}

/// Stage 3 — samples, primary-index + skip-index served (docs/schemas.md
/// §3.2). `services` are pre-escaped string literals; `line_filters` are
/// pre-rendered predicate fragments (see
/// [`super::plan::compile_line_filter`]), one per pipeline `LineFilter`
/// stage, ANDed together.
///
/// **Singleton/`IN` split (architect plan amendment §2, review finding 2):**
/// exactly one service renders the byte-exact §3.2 form `PREWHERE service =
/// 'checkout'`; more than one renders `PREWHERE service IN (...)`.
///
/// **The `ORDER BY` is a TOTAL order, and that is load-bearing (issue
/// #406, found by a CI failure).** `ORDER BY timestamp_ns` alone leaves
/// entries sharing a timestamp in whatever order the parts happened to be
/// read in, which ClickHouse does not fix across runs: measured
/// 2026-08-10 on 81 active parts holding 200 rows at one identical
/// `timestamp_ns`, **fifteen identical queries returned fifteen different
/// orderings**, and at a `LIMIT` that cut through the tie group,
/// **fifteen different result SETS** — the same query answering with
/// different log lines each time. The reference is stable over the same
/// corpus shape (229 rows, 160 separate appends): one ordering, one set,
/// every run.
///
/// So ties break on `fingerprint`, then `cityHash64(body)`, then the raw
/// `body` — **the identical key list [`stage3_keyset`] already renders**,
/// so the fast path and the fetch-until-limit path order a tie group the
/// same way and a pipeline gaining a dropping stage cannot reshuffle it.
/// All four columns follow `direction`, as there.
///
/// This costs no pruning: the table's sort key is `(service, fingerprint,
/// timestamp_ns)` and this query leads with `timestamp_ns`, so it was
/// never a read-in-order scan — the added keys change comparison cost
/// inside a sort that already had to happen, over at most the matched
/// rows. Index engagement is pinned unchanged by
/// `explain_indexes.rs`'s stage-3 cases.
///
/// **This buys determinism, NOT the reference's order.** Both stores are
/// deterministic and they settle into different sequences — ours is
/// `cityHash64(body)`, the reference's is arrival order within a stream
/// and `streamHash` across streams. At a `LIMIT` cutting a tie group that
/// means a different subset survives, so it is registered as
/// `timestamp-tie-order` in docs/benchmarks/logs-differential-ledger.md
/// with the probe, both sequences, and why matching it is separate work.
#[allow(clippy::too_many_arguments)]
pub fn stage3(
    tenant: &Tenant,
    samples_table: &str,
    services: &[CheckedLiteral],
    fingerprints: &[FpLiteral],
    window: TimeWindow,
    line_filters: &[CheckedFragment],
    direction: Direction,
    limit: u32,
) -> String {
    stage3_with_rx(
        tenant,
        samples_table,
        services,
        fingerprints,
        window,
        line_filters,
        direction,
        limit,
        None,
    )
}

/// `,\n       <rx> AS rx`, the `rx` column after a raw read's column list
/// (issue #624, part 3a, D6), or nothing.
fn rx_projection(rx: Option<&CheckedFragment>) -> String {
    match rx {
        Some(rx) => format!(",\n       {} AS rx", rx.as_sql()),
        None => String::new(),
    }
}

/// [`stage3`], with the `rx` column of the `regexp` stages the database
/// runs (issue #624, part 3a, D6) after the column list. `None` renders
/// [`stage3`]'s text byte for byte.
#[allow(clippy::too_many_arguments)]
pub fn stage3_with_rx(
    tenant: &Tenant,
    samples_table: &str,
    services: &[CheckedLiteral],
    fingerprints: &[FpLiteral],
    window: TimeWindow,
    line_filters: &[CheckedFragment],
    direction: Direction,
    limit: u32,
    rx: Option<&CheckedFragment>,
) -> String {
    let t = tenant.sql_literal();
    let service_pred = service_predicate(services);
    let fp_list = fp_list(fingerprints);
    let order = match direction {
        Direction::Backward => "DESC",
        Direction::Forward => "ASC",
    };
    let TimeWindow { start_ns, end_ns } = window;
    let rx = rx_projection(rx);

    let mut sql = format!(
        "SELECT fingerprint, timestamp_ns, body, structured_metadata{rx}\nFROM {samples_table}\nPREWHERE {service_pred}\nWHERE org_id = {t} AND fingerprint IN ({fp_list})\n  AND timestamp_ns > {start_ns} AND timestamp_ns <= {end_ns}"
    );
    for clause in line_filters {
        sql.push_str("\n  AND ");
        sql.push_str(clause.as_sql());
    }
    sql.push_str(&format!(
        "\nORDER BY timestamp_ns {order}, fingerprint {order}, cityHash64(body) {order}, body \
         {order}\nLIMIT {limit}"
    ));
    sql
}

/// The lower-bound mode of one [`stage3_keyset`] page (issue #74 live
/// tail, generalized for issue #90 streams paging — plan v4 D1/D2 + the
/// round-4 adjudication).
///
/// - [`KeysetLower::First`] — the first page: the API window's exclusive
///   `start`/inclusive `end` bounds (`timestamp_ns > start_ns AND
///   timestamp_ns <= end_ns`, docs/schemas.md §3.2), carried by the
///   `window` argument. No keyset term. Direction-agnostic.
/// - [`KeysetLower::After`] — every later page: the occurrence-count
///   keyset. The composite predicate is **inclusive** (`>=` the boundary
///   tuple when walking Forward, `<=` when walking Backward) so a tie
///   group split by `LIMIT` is re-fetched rather than skipped, and the
///   server-side `OFFSET` skips exactly the already-delivered rows of the
///   boundary tuple (deterministic under the total `ORDER BY` below). The
///   redundant `timestamp_ns >= ts` (Forward) / `timestamp_ns <= ts`
///   (Backward) term keeps the primary index's time column engaged for
///   granule pruning (the tuple comparison alone does not prune).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeysetLower {
    /// The first page; the `window` argument carries both time bounds.
    First,
    After {
        /// The boundary `(timestamp_ns, fingerprint, cityHash64(body))`.
        /// The fingerprint is a minted literal: this is a tuple
        /// comparison, and a bare decimal above `2^64` re-delivers the
        /// boundary row walking forward and skips it walking back (issue
        /// #498).
        tuple: (i64, FpLiteral, u64),
        /// How many rows equal to `tuple` were already delivered — the
        /// SQL `OFFSET`.
        offset: u32,
    },
}

/// Stage 3, keyset-pagination mode (issue #74 live tail; issue #90
/// streams fetch-until-limit — deliberately a reusable builder, the
/// foundation for streams pagination, not a tail-only hack). Leaves the
/// frozen [`stage3`] untouched; shares its `PREWHERE
/// service`/`fingerprint IN`/line-filter pushdown contract byte-for-byte.
///
/// The total `ORDER BY timestamp_ns, fingerprint, body_hash, body` (raw
/// `body` as final tiebreaker), all columns following `direction`, makes
/// equal-`(ts, fp, hash)` rows — including genuine CityHash collisions —
/// a stable adjacent run across queries, so [`KeysetLower::After`]'s
/// occurrence-count `OFFSET` is well-defined. `cityHash64(body)` is
/// projected by ClickHouse and captured as `body_hash` (no CH/Rust
/// divergence at the boundary).
///
/// **`direction` mirrors the whole page.** Forward (ASC, tail's only
/// mode — byte-identical to issue #74's rendering) walks oldest→newest
/// with a `>=` composite and the redundant `timestamp_ns >= ts` lower
/// bound; Backward (DESC, the query default's newest-first mode) walks
/// newest→oldest with a `<=` composite and the redundant `timestamp_ns
/// <= ts` upper bound, keeping the API's `start` bound as the fixed
/// lower.
// One arg over clippy's threshold: `window`, `lower`, and `direction`
// are each independent page coordinates that cannot fold without
// obscuring the SQL contract (the sibling `stage3` sits at the 7-arg
// limit for the same reason).
#[allow(clippy::too_many_arguments)]
pub fn stage3_keyset(
    tenant: &Tenant,
    samples_table: &str,
    services: &[CheckedLiteral],
    fingerprints: &[FpLiteral],
    window: TimeWindow,
    lower: KeysetLower,
    direction: Direction,
    line_filters: &[CheckedFragment],
    limit: u32,
) -> String {
    stage3_keyset_with_rx(
        tenant,
        samples_table,
        services,
        fingerprints,
        window,
        lower,
        direction,
        line_filters,
        limit,
        None,
    )
}

/// [`stage3_keyset`], with the `rx` column (issue #624, part 3a, D6) after
/// the column list. `None` renders [`stage3_keyset`]'s text byte for byte.
#[allow(clippy::too_many_arguments)]
pub fn stage3_keyset_with_rx(
    tenant: &Tenant,
    samples_table: &str,
    services: &[CheckedLiteral],
    fingerprints: &[FpLiteral],
    window: TimeWindow,
    lower: KeysetLower,
    direction: Direction,
    line_filters: &[CheckedFragment],
    limit: u32,
    rx: Option<&CheckedFragment>,
) -> String {
    let t = tenant.sql_literal();
    let service_pred = service_predicate(services);
    let fp_list = fp_list(fingerprints);
    let TimeWindow { start_ns, end_ns } = window;
    let rx = rx_projection(rx);

    let mut sql = format!(
        "SELECT fingerprint, timestamp_ns, body, cityHash64(body) AS body_hash, structured_metadata{rx}\nFROM {samples_table}\nPREWHERE {service_pred}\nWHERE org_id = {t} AND fingerprint IN ({fp_list})"
    );
    match (direction, lower) {
        (_, KeysetLower::First) => {
            sql.push_str(&format!(
                "\n  AND timestamp_ns > {start_ns} AND timestamp_ns <= {end_ns}"
            ));
        }
        (
            Direction::Forward,
            KeysetLower::After {
                tuple: (ts, fp, hash),
                ..
            },
        ) => {
            sql.push_str(&format!(
                "\n  AND timestamp_ns >= {ts} AND timestamp_ns <= {end_ns}\n  AND (timestamp_ns, fingerprint, cityHash64(body)) >= ({ts}, {fp}, {hash})"
            ));
        }
        (
            Direction::Backward,
            KeysetLower::After {
                tuple: (ts, fp, hash),
                ..
            },
        ) => {
            sql.push_str(&format!(
                "\n  AND timestamp_ns > {start_ns} AND timestamp_ns <= {ts}\n  AND (timestamp_ns, fingerprint, cityHash64(body)) <= ({ts}, {fp}, {hash})"
            ));
        }
    }
    for clause in line_filters {
        sql.push_str("\n  AND ");
        sql.push_str(clause.as_sql());
    }
    let ord = match direction {
        Direction::Forward => "ASC",
        Direction::Backward => "DESC",
    };
    sql.push_str(&format!(
        "\nORDER BY timestamp_ns {ord}, fingerprint {ord}, body_hash {ord}, body {ord}"
    ));
    match lower {
        KeysetLower::First => sql.push_str(&format!("\nLIMIT {limit}")),
        KeysetLower::After { offset, .. } => {
            sql.push_str(&format!("\nLIMIT {limit} OFFSET {offset}"))
        }
    }
    sql
}

/// The `/api/logs/v1/stats` rollup-served aggregation (issue #74, no line
/// filter): zero body reads — `sum(count)`/`sum(bytes)` off
/// `log_metrics_<res>` (PK `(fingerprint, bucket_ns)`), `streams` as
/// `uniqExact(fingerprint)`, and `chunks` as the adjudicated
/// selector-scoped partition-count proxy (`uniqExact` of the bucket's
/// date — docs/api.md §2.5). Same half-open bucket predicate as
/// [`metric_range`].
pub fn log_stats_rollup(
    tenant: &Tenant,
    rollup_table: &str,
    fingerprints: &[FpLiteral],
    window: TimeWindow,
) -> String {
    let t = tenant.sql_literal();
    let fp_list = fp_list(fingerprints);
    let TimeWindow { start_ns, end_ns } = window;
    format!(
        "SELECT uniqExact(fingerprint) AS streams, uniqExact(toDate(fromUnixTimestamp64Nano(bucket_ns))) AS chunks, sum(count) AS entries, sum(bytes) AS bytes\nFROM {rollup_table}\nWHERE org_id = {t} AND fingerprint IN ({fp_list}) AND bucket_ns > {start_ns} AND bucket_ns <= {end_ns}"
    )
}

/// The `/api/logs/v1/stats` raw fallback (issue #74, line-filtered): the
/// rollup is body-content-blind, so a line filter forces a `log_samples`
/// scan with the identical `PREWHERE service` + skip-index-prunable
/// line-filter predicates [`stage3`] emits (granule-skipped, PK-pruned). Same
/// `streams`/`chunks` shape as [`log_stats_rollup`]; `entries`/`bytes`
/// count matching lines exactly.
pub fn log_stats_raw(
    tenant: &Tenant,
    samples_table: &str,
    services: &[CheckedLiteral],
    fingerprints: &[FpLiteral],
    window: TimeWindow,
    line_filters: &[CheckedFragment],
) -> String {
    let t = tenant.sql_literal();
    let service_pred = service_predicate(services);
    let fp_list = fp_list(fingerprints);
    let TimeWindow { start_ns, end_ns } = window;
    let mut sql = format!(
        "SELECT uniqExact(fingerprint) AS streams, uniqExact(toDate(fromUnixTimestamp64Nano(timestamp_ns))) AS chunks, count() AS entries, sum(length(body)) AS bytes\nFROM {samples_table}\nPREWHERE {service_pred}\nWHERE org_id = {t} AND fingerprint IN ({fp_list})\n  AND timestamp_ns > {start_ns} AND timestamp_ns <= {end_ns}"
    );
    for clause in line_filters {
        sql.push_str("\n  AND ");
        sql.push_str(clause.as_sql());
    }
    sql
}

/// The `/api/logs/v1/volume` aggregation (issue #169, docs/api.md §2.6):
/// per-fingerprint byte volume off `log_metrics_<res>` (PK `(fingerprint,
/// bucket_ns)`) — rollup-ONLY, zero body reads (the endpoint accepts a
/// matchers-only selector, so there is no raw fallback at all, unlike
/// [`log_stats_rollup`]'s line-filtered sibling). Same half-open bucket
/// predicate family as [`log_stats_rollup`]/[`metric_range`], so the
/// identical MinMax + `(fingerprint, bucket_ns)` primary-key pruning
/// applies (`tests/explain_indexes.rs`' Tier-1 gate).
pub fn log_volume_rollup(
    tenant: &Tenant,
    rollup_table: &str,
    fingerprints: &[FpLiteral],
    window: TimeWindow,
) -> String {
    let t = tenant.sql_literal();
    let fp_list = fp_list(fingerprints);
    let TimeWindow { start_ns, end_ns } = window;
    format!(
        "SELECT fingerprint, sum(bytes) AS bytes\nFROM {rollup_table}\nWHERE org_id = {t} AND fingerprint IN ({fp_list}) AND bucket_ns > {start_ns} AND bucket_ns <= {end_ns}\nGROUP BY fingerprint"
    )
}

/// The maximum number of pattern series `/api/logs/v1/patterns` returns — the
/// top-`N`-by-total-count LIMIT pushed into ClickHouse (M7-C3, issue #171).
pub const MAX_PATTERNS: usize = 1000;

/// The `/api/logs/v1/patterns` aggregation (M7-C3, issue #171, docs/schemas.md
/// §3.2): stage-1 fingerprints → ONE pushed-down aggregate over `log_patterns`
/// with no hydration (the response carries no labels). Per `(fingerprint, pattern, level)` it pairs
/// consecutive rows; at a step other than 10s a pair under an hour apart marks the empty steps
/// between them (issue #605, the reference's chunk fill). It sums per `(pattern, level, ts_ns)`,
/// takes the top-[`MAX_PATTERNS`] by total, pattern, level, and only then writes their zeros.
///
/// **Pushdown/pruning:** `fingerprint IN` engages the `(fingerprint, bucket_ns,
/// pattern)` primary-key prefix (granule pruning), daily partitions prune the
/// window (`tests/explain_indexes.rs`' Tier-1 gate), and the aggregation +
/// top-K + LIMIT all execute in ClickHouse — the client decodes ≤ 1000
/// already-assembled series. Half-open window `[start, end)` (D4).
pub fn log_patterns_read(
    tenant: &Tenant,
    patterns_table: &str,
    fingerprints: &[FpLiteral],
    window: TimeWindow,
    step_ns: u64,
) -> String {
    let t = tenant.sql_literal();
    let fp_list = fp_list(fingerprints);
    let TimeWindow { start_ns, end_ns } = window;
    let limit = MAX_PATTERNS;
    format!(
        "SELECT pattern, level, total, arrayFlatten(arrayMap((p, c, n) -> arrayConcat(if(p.2 > 0 OR c > 0, [(p.1, p.2)], []), if(c > 0, arrayMap(z -> (toInt64(z), toUInt64(0)), range(p.1 + {step_ns}, n, {step_ns})), [])), pts, arrayCumSum(arrayMap(p -> p.3, pts)), arrayPushBack(arrayPopFront(arrayMap(p -> p.1, pts)), 0))) AS samples\nFROM (\n  SELECT pattern, level, sum(cnt) AS total, arraySort(groupArray((ts_ns, cnt, delta))) AS pts\n  FROM (\n    SELECT pattern, level, point.1 AS ts_ns, sum(point.2) AS cnt, sum(point.3) AS delta\n    FROM (\n      SELECT pattern, level, arraySort(groupArray((bucket_ns, toUInt64(count)))) AS rows\n      FROM {patterns_table}\n      WHERE org_id = {t} AND fingerprint IN ({fp_list}) AND bucket_ns >= {start_ns} AND bucket_ns < {end_ns}\n      GROUP BY fingerprint, pattern, level\n    )\n    ARRAY JOIN arrayConcat(arrayMap(r -> (intDiv(r.1, {step_ns}) * {step_ns}, r.2, toInt64(0)), rows), arrayFlatten(arrayMap((a, b) -> if(b.1 - a.1 < if({step_ns} = 10000000000, 0, 3600000000000) AND intDiv(a.1, {step_ns}) * {step_ns} + {step_ns} < intDiv(b.1, {step_ns}) * {step_ns}, [(intDiv(a.1, {step_ns}) * {step_ns} + {step_ns}, toUInt64(0), toInt64(1)), (intDiv(b.1, {step_ns}) * {step_ns}, toUInt64(0), toInt64(-1))], []), arrayPopBack(rows), arrayPopFront(rows)))) AS point\n    GROUP BY pattern, level, ts_ns\n  )\n  GROUP BY pattern, level\n  ORDER BY total DESC, pattern ASC, level ASC\n  LIMIT {limit}\n)\nORDER BY total DESC, pattern ASC, level ASC"
    )
}

/// A range metric query bucketed by `step_ns` (`intDiv(bucket_col, step) *
/// step`, docs/schemas.md §3.2). `extra_predicates` carries line-filter
/// pushdown for the (line-filter-forced) raw fallback.
///
/// **`PREWHERE service ...` on the raw fallback only (fix-plan amendment
/// §3, code review finding "Raw metric fallback loses the `log_samples`
/// primary-key prefix"):** when `source.table` is `log_samples`, omitting a
/// service predicate drops the leading column of `ORDER BY (service,
/// fingerprint, timestamp_ns)` — docs/schemas.md §3.2 line 285 mandates
/// injecting it "even a query that never mentions `service`" to keep the
/// primary index engaged, exactly as stage 3 already does. Pass `services =
/// &[]` for the rollup path (`log_metrics_<res>` has no `service` column,
/// `ORDER BY (fingerprint, bucket_ns)`); a non-empty `services` renders the
/// same singleton/`IN` split [`stage3`] uses.
///
/// **No read path calls it** (issue #614). A range read runs the bucketed
/// statement, the sliding one, or — on the rollup route — the rollup
/// statement ([`metric_range_rollup`]), whose windows are the reference's
/// `(t - range, t]`. This floor-grid shape is not: it counts `[t, t +
/// step)` and buckets an already-bucketed column. It is kept for its
/// snapshot tests and the benchmark harness, which pin its text, and no
/// planner route reaches it.
#[allow(clippy::too_many_arguments)]
pub fn metric_range(
    tenant: &Tenant,
    source: MetricSource<'_>,
    services: &[CheckedLiteral],
    fingerprints: &[FpLiteral],
    window: TimeWindow,
    lower: ScanLowerBound,
    step_ns: u64,
    extra_predicates: &[CheckedFragment],
) -> String {
    let t = tenant.sql_literal();
    let MetricSource { table, shape } = source;
    let (bucket_col, agg_expr) = (shape.bucket_col(), shape.agg_expr());
    let fp_list = fp_list(fingerprints);
    let TimeWindow { start_ns, end_ns } = window;
    let lower_op = lower.sql_op();
    let prewhere = metric_prewhere(services);
    let mut sql = format!(
        "SELECT fingerprint, intDiv({bucket_col}, {step_ns}) * {step_ns} AS step, {agg_expr} AS n\nFROM {table}\n{prewhere}WHERE org_id = {t} AND fingerprint IN ({fp_list}) AND {bucket_col} {lower_op} {start_ns} AND {bucket_col} <= {end_ns}"
    );
    for clause in extra_predicates {
        sql.push_str(" AND ");
        sql.push_str(clause.as_sql());
    }
    sql.push_str("\nGROUP BY fingerprint, step");
    sql
}

/// An instant metric query — a single window, no bucketing
/// ([`super::params::QuerySpec::Instant`]'s structural contract: no
/// `intDiv` expression, no `step` column). See [`metric_range`]'s doc
/// comment for the `services`/`PREWHERE` contract (fix-plan amendment §3).
///
/// **Grouped by `(fingerprint, structured_metadata)`** under
/// [`ScanProjection::WithStructuredMetadata`] (issue #249): the metric path
/// merges structured metadata into the label set, so one fingerprint covers
/// N output series, and the client re-groups the returned rows by the merged
/// final label set. That re-grouping is EXACT rather than approximate
/// because every op that can reach this pushdown path is a linear sum
/// (`count()` / `sum(length(body))`), so summing the server's per-metadata
/// partial counts reproduces the client path's single accumulator bit for
/// bit.
///
/// [`ScanProjection::Lean`] exists here for the ROLLUP source
/// (`log_metrics_<res>`), which has no `structured_metadata` column at all.
/// That arm is structurally unreachable for a LogQL instant plan — a
/// `RouteChoice::Rollup` decision requires `QuerySpec::Range` (`plan.rs`'s
/// `match p.spec` on the routing decision), so an instant query is always
/// `Raw` over `log_samples`. The parameter exists so the builder CANNOT
/// render a column the named table lacks, rather than resting on that
/// reachability argument holding forever.
#[allow(clippy::too_many_arguments)]
pub fn metric_instant(
    tenant: &Tenant,
    source: MetricSource<'_>,
    services: &[CheckedLiteral],
    fingerprints: &[FpLiteral],
    window: TimeWindow,
    lower: ScanLowerBound,
    extra_predicates: &[CheckedFragment],
    projection: ScanProjection,
) -> String {
    let t = tenant.sql_literal();
    let MetricSource { table, shape } = source;
    let (bucket_col, agg_expr) = (shape.bucket_col(), shape.agg_expr());
    let fp_list = fp_list(fingerprints);
    let TimeWindow { start_ns, end_ns } = window;
    let lower_op = lower.sql_op();
    let prewhere = metric_prewhere(services);
    let sm = projection.column_suffix();
    let mut sql = format!(
        "SELECT fingerprint, {agg_expr} AS n{sm}\nFROM {table}\n{prewhere}WHERE org_id = {t} AND fingerprint IN ({fp_list}) AND {bucket_col} {lower_op} {start_ns} AND {bucket_col} <= {end_ns}"
    );
    for clause in extra_predicates {
        sql.push_str(" AND ");
        sql.push_str(clause.as_sql());
    }
    sql.push_str(match projection {
        ScanProjection::Lean => "\nGROUP BY fingerprint",
        ScanProjection::WithStructuredMetadata => "\nGROUP BY fingerprint, structured_metadata",
    });
    sql
}

/// Whether a raw metric scan projects `structured_metadata` (issue #249).
///
/// The metric path merges structured metadata into the label set, so it must
/// normally be read. [`ScanProjection::Lean`] has no caller on a read path
/// since issue #624 part 2. Its one caller was `absent_over_time`, whose
/// label set cannot depend on metadata (`pkg/logql/syntax/extractor.go:46-47
/// @ v3.7.4` forces `noLabels = true` for `OpRangeTypeAbsent`); but every raw
/// scan decodes into the one row type, which carries the column, so the
/// lean read failed to decode wherever the selector resolved a stream. The
/// variant stays for the builders it shapes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanProjection {
    /// No `structured_metadata` column; no read path takes it.
    Lean,
    /// Every other metric reducer.
    WithStructuredMetadata,
}

impl ScanProjection {
    /// The `SELECT` list's trailing column, or nothing.
    fn column_suffix(self) -> &'static str {
        match self {
            ScanProjection::Lean => "",
            ScanProjection::WithStructuredMetadata => ", structured_metadata",
        }
    }
}

/// The client-aggregated metric fetch (issue M6-10): a stage-3-shaped raw
/// scan of `(fingerprint, timestamp_ns, body)` over the **full** window,
/// with the line-filter prefix pushed down — and deliberately **no
/// `LIMIT`**: an aggregation must see every matching line or abort on the
/// byte scan budget (`max_bytes_to_read` → `QueryTooBroad`), never
/// silently truncate (complete-or-error, the adjudicated design). The
/// `PREWHERE service ...` contract matches [`stage3`]/[`metric_range`]
/// (the `log_samples` primary-key prefix stays engaged).
///
/// **Stable total order (review round 2, finding 2):** `ORDER BY`
/// carries `fingerprint, body` as secondary keys — the projection's only
/// other columns — so equal-timestamp rows arrive in one reproducible
/// order across runs/merges/replicas (float accumulation order, and
/// therefore bit-level sums, stay stable).
///
/// **This ordering is now load-bearing for `first`/`last`, not merely
/// stabilising** (issue #344). Those reducers take the endpoints of
/// Loki's `(timestamp, stream_hash, tie_rank)` delivery order; the
/// instant accumulator compares `(timestamp, stream_hash)` explicitly
/// and reads `tie_rank` — which separates only SAME-stream samples at
/// one nanosecond — off the arrival sequence this `ORDER BY` produces,
/// `body` ascending being exactly the sliding path's group-local
/// `tie_rank`. Dropping or reordering the `body` key would therefore
/// change answers, not just bit-level sums. (The doc line replaced here
/// claimed the first/last reducers were "additionally order-independent
/// via their own value tie-break"; that tie-break was deleted as a wrong
/// value — see `SimpleAcc::add`.)
///
/// **`structured_metadata` is in the `SELECT` list and NOWHERE else** (issue
/// #249). It never appears in `WHERE`, `PREWHERE`, `ORDER BY` or any skip
/// index: a filter on a metadata label is evaluated CLIENT-side, in the
/// compiled pipeline, over the merged label set. That is the reference's
/// shape too — a metadata filter is a pipeline stage run per entry, after
/// `builder.Add(StructuredMetadataLabel, …)` at
/// `pkg/logql/log/metrics_extraction.go:104 @ v3.7.4`, and
/// `grep -n StructuredMetadata pkg/logql/syntax/*.go` at `b318f28` returns
/// no filter, matcher or pushdown site at all. Pushing it would also be
/// useless AND wrong: the column is an opaque canonical-JSON `String` with
/// `DEFAULT ''` and no index (`pulsus-schema/src/catalog.rs`), so a
/// JSON-extract predicate could prune no granule, and the merge renames a
/// colliding key to `<k>_extracted` before any filter sees it.
#[allow(clippy::too_many_arguments)]
pub fn metric_raw_samples(
    tenant: &Tenant,
    samples_table: &str,
    services: &[CheckedLiteral],
    fingerprints: &[FpLiteral],
    window: TimeWindow,
    lower: ScanLowerBound,
    extra_predicates: &[CheckedFragment],
    projection: ScanProjection,
) -> String {
    metric_raw_samples_with_rx(
        tenant,
        samples_table,
        services,
        fingerprints,
        window,
        lower,
        extra_predicates,
        projection,
        None,
    )
}

/// [`metric_raw_samples`], with the `rx` column (issue #624, part 3a, D6)
/// after the column list. `None` renders [`metric_raw_samples`]'s text byte
/// for byte.
#[allow(clippy::too_many_arguments)]
pub fn metric_raw_samples_with_rx(
    tenant: &Tenant,
    samples_table: &str,
    services: &[CheckedLiteral],
    fingerprints: &[FpLiteral],
    window: TimeWindow,
    lower: ScanLowerBound,
    extra_predicates: &[CheckedFragment],
    projection: ScanProjection,
    rx: Option<&CheckedFragment>,
) -> String {
    let t = tenant.sql_literal();
    let service_pred = service_predicate(services);
    let fp_list = fp_list(fingerprints);
    let TimeWindow { start_ns, end_ns } = window;
    let lower_op = lower.sql_op();
    let sm = projection.column_suffix();
    let rx = rx_projection(rx);
    let mut sql = format!(
        "SELECT fingerprint, timestamp_ns, body{sm}{rx}\nFROM {samples_table}\nPREWHERE {service_pred}\nWHERE org_id = {t} AND fingerprint IN ({fp_list})\n  AND timestamp_ns {lower_op} {start_ns} AND timestamp_ns <= {end_ns}"
    );
    for clause in extra_predicates {
        sql.push_str("\n  AND ");
        sql.push_str(clause.as_sql());
    }
    sql.push_str("\nORDER BY timestamp_ns ASC, fingerprint ASC, body ASC");
    sql
}

/// The sliding-window range metric fetch (issue #227): identical scan/
/// predicate shape to [`metric_raw_samples`], but ordered by the **physical
/// primary key** `(service, fingerprint, timestamp_ns)` so ClickHouse
/// streams it with `optimize_read_in_order=1` and **no server-side sort** —
/// the memory-bound streaming slide reads one fingerprint's ascending-ts run
/// at a time. Deliberately **no `body` in `ORDER BY`** (body is not in the
/// MergeTree key, so ordering by it would force an unbounded per-collision-
/// group sort — issue #227 review finding): same-`(fingerprint,
/// timestamp_ns)` rows arrive in arbitrary order and the deterministic
/// full-body `tie_rank` order is imposed in Rust at group formation. And
/// deliberately **no global `ORDER BY timestamp_ns`** (a scan-sized sort;
/// the canonical `(ts, stream_hash, tie_rank)` fold order is imposed in
/// Rust over the bounded retained window).
///
/// **And deliberately no `structured_metadata` in `ORDER BY`** (issue #249).
/// It is a projected column only — see [`metric_raw_samples`]'s note for why
/// it is never a predicate either. Adding it to the sort key would make
/// `(service, fingerprint, structured_metadata, timestamp_ns)` a
/// NON-primary-key order and force a scan-sized server-side sort, destroying
/// `optimize_read_in_order`. The design does not need it: the fan-out
/// `MutCells` representation is order-independent by construction, and rows
/// within one metadata variant still arrive timestamp-ascending because the
/// existing key order already delivers them that way.
#[allow(clippy::too_many_arguments)]
pub fn metric_raw_samples_sliding(
    tenant: &Tenant,
    samples_table: &str,
    services: &[CheckedLiteral],
    fingerprints: &[FpLiteral],
    window: TimeWindow,
    lower: ScanLowerBound,
    extra_predicates: &[CheckedFragment],
    projection: ScanProjection,
) -> String {
    metric_raw_samples_sliding_with_rx(
        tenant,
        samples_table,
        services,
        fingerprints,
        window,
        lower,
        extra_predicates,
        projection,
        None,
    )
}

/// [`metric_raw_samples_sliding`], with the `rx` column (issue #624, part
/// 3a, D6) after the column list. `None` renders
/// [`metric_raw_samples_sliding`]'s text byte for byte.
#[allow(clippy::too_many_arguments)]
pub fn metric_raw_samples_sliding_with_rx(
    tenant: &Tenant,
    samples_table: &str,
    services: &[CheckedLiteral],
    fingerprints: &[FpLiteral],
    window: TimeWindow,
    lower: ScanLowerBound,
    extra_predicates: &[CheckedFragment],
    projection: ScanProjection,
    rx: Option<&CheckedFragment>,
) -> String {
    let t = tenant.sql_literal();
    let service_pred = service_predicate(services);
    let fp_list = fp_list(fingerprints);
    let TimeWindow { start_ns, end_ns } = window;
    let lower_op = lower.sql_op();
    let sm = projection.column_suffix();
    let rx = rx_projection(rx);
    let mut sql = format!(
        "SELECT fingerprint, timestamp_ns, body{sm}{rx}\nFROM {samples_table}\nPREWHERE {service_pred}\nWHERE org_id = {t} AND fingerprint IN ({fp_list})\n  AND timestamp_ns {lower_op} {start_ns} AND timestamp_ns <= {end_ns}"
    );
    for clause in extra_predicates {
        sql.push_str("\n  AND ");
        sql.push_str(clause.as_sql());
    }
    sql.push_str("\nORDER BY service ASC, fingerprint ASC, timestamp_ns ASC");
    sql
}

/// The scan bounds and the emit grid one bucketed range read is rendered
/// from (issue #507, W2), grouped into one parameter for the same reason
/// [`TimeWindow`] is.
///
/// `lo_ns` is `grid_start_ns - step_ns` — the plan's start-anchored emit
/// grid, one step below its first point — and **not** `window.start_ns`,
/// which is the scan start and has been widened backwards by the range
/// selector's duration. They are equal only when the range equals the
/// step.
#[derive(Debug, Clone, Copy)]
pub struct BucketedScan {
    pub window: TimeWindow,
    pub lower: ScanLowerBound,
    pub lo_ns: i64,
    pub step_ns: i64,
}

/// The bucketed range metric read (issue #507, W2): one row per
/// `(fingerprint, grid point, structured_metadata)` instead of one row per
/// log line.
///
/// **What it is for.** A range aggregation over a counting reducer is a
/// per-window count, and the database can count. Today every range query
/// reads every matching line across the metered hop and counts them in our
/// process; this returns the counts.
///
/// **Why the grid column is not the shipped one.** [`metric_range`] buckets
/// with `intDiv(<col>, step) * step`, a floor onto a grid anchored at the
/// epoch. The reference's window is `(g - range, g]` on a grid anchored at
/// the query's start, which is a ceiling onto that grid — see
/// [`super::predicate::bucket_expr`], which mints the expression and holds
/// the reason. `metric_range` is untouched: it has no production caller,
/// and its rollup half buckets an already-bucketed column, which is a
/// different question nobody has answered.
///
/// **`structured_metadata` is carried unconditionally**, in the `SELECT`
/// list and the `GROUP BY`, in the trailing position [`metric_instant`]
/// puts it in. It is part of the output series identity even when no error
/// is involved — two entries of one stream differing only in a metadata
/// value are two series, and dropping the column would merge them into one
/// of twice the value. **No expression here interprets the column**: it is
/// named and grouped, never read into, and the one reader decides what it
/// means after the rows arrive.
///
/// **Raw source only.** The rollup table has no `structured_metadata`
/// column at all, so a rollup shape cannot reach this builder with the
/// projection it needs. The `bucket_col` the grid is computed over is the
/// shape's own, so the `WHERE` bound and the grid always name one column.
pub fn metric_range_bucketed(
    tenant: &Tenant,
    source: MetricSource<'_>,
    services: &[CheckedLiteral],
    fingerprints: &[FpLiteral],
    scan: BucketedScan,
    extra_predicates: &[CheckedFragment],
    projection: ScanProjection,
) -> Result<String, super::predicate::BucketGridRefusal> {
    metric_range_bucketed_with_regexp(
        tenant,
        source,
        services,
        fingerprints,
        scan,
        extra_predicates,
        projection,
        None,
    )
}

/// [`metric_range_bucketed`] over a `regexp` stage (issue #624, part 3a,
/// D2): the extraction is one level under the statement's `SELECT`, and
/// `matched, caps` join `fingerprint, structured_metadata` in the `GROUP
/// BY`. `None` renders [`metric_range_bucketed`]'s text byte for byte.
#[allow(clippy::too_many_arguments)]
pub fn metric_range_bucketed_with_regexp(
    tenant: &Tenant,
    source: MetricSource<'_>,
    services: &[CheckedLiteral],
    fingerprints: &[FpLiteral],
    scan: BucketedScan,
    extra_predicates: &[CheckedFragment],
    projection: ScanProjection,
    regexp: Option<GroupExtraction<'_>>,
) -> Result<String, super::predicate::BucketGridRefusal> {
    let t = tenant.sql_literal();
    let MetricSource { table, shape } = source;
    let (bucket_col, agg_expr) = (shape.bucket_col(), shape.agg_expr());
    let BucketedScan {
        window,
        lower,
        lo_ns,
        step_ns,
    } = scan;
    let TimeWindow { start_ns, end_ns } = window;
    let bucket = super::predicate::bucket_expr(bucket_col, lo_ns, step_ns, start_ns, end_ns)?;
    let fp_list = fp_list(fingerprints);
    let lower_op = lower.sql_op();
    let prewhere = metric_prewhere(services);
    let sm = projection.column_suffix();
    let bucket_sql = bucket.as_sql();
    if let Some(regexp) = regexp {
        // The stage reads the stored line, which the rollup has not got.
        if bucket_col != "timestamp_ns" {
            return Err(super::predicate::BucketGridRefusal::RollupSource);
        }
        let mut lines: Vec<String> = vec![
            format!(
                "SELECT fingerprint, {bucket_sql} AS bucket_ns, {agg_expr} AS n, \
                 structured_metadata, {}",
                regexp.key_columns()
            ),
            "FROM (".to_string(),
            format!(
                "  SELECT fingerprint, timestamp_ns, {}structured_metadata,",
                regexp_body_column(agg_expr)
            ),
        ];
        lines.extend(regexp.lines("         "));
        lines.push(format!("  FROM {table}"));
        if !services.is_empty() {
            lines.push(format!("  PREWHERE {}", service_predicate(services)));
        }
        lines.push(format!(
            "  WHERE org_id = {t} AND fingerprint IN ({fp_list})"
        ));
        lines.push(format!(
            "    AND {bucket_col} {lower_op} {start_ns} AND {bucket_col} <= {end_ns}"
        ));
        for clause in extra_predicates {
            lines.push(format!("    AND {}", clause.as_sql()));
        }
        lines.push(")".to_string());
        lines.extend(regexp.throw_line(""));
        lines.push(format!(
            "GROUP BY fingerprint, bucket_ns, structured_metadata, {}",
            regexp.key_columns()
        ));
        return Ok(lines.join("\n"));
    }
    let mut sql = format!(
        "SELECT fingerprint, {bucket_sql} AS bucket_ns, {agg_expr} AS n{sm}\nFROM {table}\n{prewhere}WHERE org_id = {t} AND fingerprint IN ({fp_list})\n  AND {bucket_col} {lower_op} {start_ns} AND {bucket_col} <= {end_ns}"
    );
    for clause in extra_predicates {
        sql.push_str("\n  AND ");
        sql.push_str(clause.as_sql());
    }
    sql.push_str(match projection {
        ScanProjection::Lean => "\nGROUP BY fingerprint, bucket_ns",
        ScanProjection::WithStructuredMetadata => {
            "\nGROUP BY fingerprint, bucket_ns, structured_metadata"
        }
    });
    Ok(sql)
}

/// The scan bounds and the emit grid one sliding range read is rendered
/// from (issue #624), grouped into one parameter for the same reason
/// [`TimeWindow`] is. `window.start_ns` is the scan start, `grid_start_ns -
/// range_ns`; `grid_start_ns` is the emit grid's first point.
#[derive(Debug, Clone, Copy)]
pub struct SlidingScan {
    pub window: TimeWindow,
    pub lower: ScanLowerBound,
    pub grid_start_ns: i64,
    pub step_ns: i64,
    pub range_ns: i64,
}

/// The sliding range metric read (issue #624): one row per `(fingerprint,
/// grid point, structured_metadata)` whose window `(g - range, g]` holds at
/// least one line, at any range and step.
///
/// ```text
/// innermost   one row per (fingerprint, metadata, lo, hi): the grid indexes
///             lo..hi whose windows hold the line; m = count or bytes,
///             c = lines
/// deltas      +m, +c at lo and -m, -c at hi + 1
/// window      the cumulative sum of the deltas: the window total v and its
///             line count p, constant from k0 up to the next delta k1
/// outer       each run expanded to its grid points; p > 0 keeps a window
///             with a line, so a bytes total of 0 over empty lines is a point
/// ```
///
/// **Why not a window frame over buckets.** A frame is evaluated only at
/// rows that exist, and a grid point whose own bucket is empty can still
/// have a non-empty window. The deltas and the expansion reach every grid
/// point a run covers. Memory grows with series × grid points, never with
/// lines × windows. [`super::predicate::sliding_cover`] holds the grid
/// arithmetic and its exactness argument.
///
/// When the range equals the step, [`metric_range_bucketed`] is the
/// statement: smaller and faster for that case.
///
/// **`structured_metadata` is carried unconditionally**, as in
/// [`metric_range_bucketed`]: it is part of the output series identity, and
/// no expression here interprets it.
///
/// # Refusals
///
/// `RollupSource` when the shape's grid column is not `timestamp_ns` — the
/// statement reads lines, and [`metric_range_rollup`] reads the rollup —
/// and every refusal of
/// [`super::predicate::sliding_cover`].
pub fn metric_range_sliding(
    tenant: &Tenant,
    source: MetricSource<'_>,
    services: &[CheckedLiteral],
    fingerprints: &[FpLiteral],
    scan: SlidingScan,
    extra_predicates: &[CheckedFragment],
) -> Result<String, super::predicate::BucketGridRefusal> {
    metric_range_sliding_with_regexp(
        tenant,
        source,
        services,
        fingerprints,
        scan,
        extra_predicates,
        None,
    )
}

/// [`metric_range_sliding`] over a `regexp` stage (issue #624, part 3a,
/// D2): the extraction is one level under the innermost `SELECT`, and
/// `matched, caps` join `fingerprint, structured_metadata` in every `GROUP
/// BY` and window partition. `None` renders [`metric_range_sliding`]'s text
/// byte for byte.
pub fn metric_range_sliding_with_regexp(
    tenant: &Tenant,
    source: MetricSource<'_>,
    services: &[CheckedLiteral],
    fingerprints: &[FpLiteral],
    scan: SlidingScan,
    extra_predicates: &[CheckedFragment],
    regexp: Option<GroupExtraction<'_>>,
) -> Result<String, super::predicate::BucketGridRefusal> {
    let t = tenant.sql_literal();
    let MetricSource { table, shape } = source;
    if shape.bucket_col() != "timestamp_ns" {
        return Err(super::predicate::BucketGridRefusal::RollupSource);
    }
    let SlidingScan {
        window,
        lower,
        grid_start_ns,
        step_ns,
        range_ns,
    } = scan;
    let TimeWindow { start_ns, end_ns } = window;
    let cover =
        super::predicate::sliding_cover(start_ns, grid_start_ns, end_ns, step_ns, range_ns)?;
    let (lo, hi, kend) = (cover.lo.as_sql(), cover.hi.as_sql(), cover.kend);
    let agg_expr = shape.agg_expr();
    let fp_list = fp_list(fingerprints);
    let lower_op = lower.sql_op();
    // Issue #624, part 3a: a `regexp` stage's captures join the group key,
    // computed one level under the innermost `SELECT`.
    let key = match regexp {
        Some(x) => format!("fingerprint, structured_metadata, {}", x.key_columns()),
        None => "fingerprint, structured_metadata".to_string(),
    };
    let mut lines: Vec<String> = vec![
        format!(
            "SELECT fingerprint, toInt64({grid_start_ns} + k * {step_ns}) AS bucket_ns, \
             toUInt64(v) AS n, structured_metadata{}",
            match regexp {
                Some(x) => format!(", {}", x.key_columns()),
                None => String::new(),
            }
        ),
        "FROM (".to_string(),
        format!("  SELECT {key}, k0,"),
        format!(
            "         sum(dv) OVER cum AS v, sum(dc) OVER cum AS p, \
             leadInFrame(k0, 1, {kend}) OVER whole AS k1"
        ),
        "  FROM (".to_string(),
        format!("    SELECT {key}, d.1 AS k0, sum(d.2) AS dv, sum(d.3) AS dc"),
        "    FROM (".to_string(),
        format!("      SELECT {key},"),
        format!("             {lo} AS lo,"),
        format!("             {hi} AS hi,"),
        format!("             {agg_expr} AS m, count() AS c"),
    ];
    let (inner, filter) = match regexp {
        Some(regexp) => {
            lines.push("      FROM (".to_string());
            lines.push(format!(
                "        SELECT fingerprint, structured_metadata, timestamp_ns{},",
                if regexp_body_column(agg_expr).is_empty() {
                    ""
                } else {
                    ", body"
                }
            ));
            lines.extend(regexp.lines("               "));
            lines.push(format!("        FROM {table}"));
            ("        ", "          ")
        }
        None => {
            lines.push(format!("      FROM {table}"));
            ("      ", "        ")
        }
    };
    if !services.is_empty() {
        lines.push(format!("{inner}PREWHERE {}", service_predicate(services)));
    }
    lines.push(format!(
        "{inner}WHERE org_id = {t} AND fingerprint IN ({fp_list})"
    ));
    lines.push(format!(
        "{filter}AND timestamp_ns {lower_op} {start_ns} AND timestamp_ns <= {end_ns}"
    ));
    for clause in extra_predicates {
        lines.push(format!("{filter}AND {}", clause.as_sql()));
    }
    if let Some(x) = regexp {
        lines.push("      )".to_string());
        lines.extend(x.throw_line("      "));
    }
    lines.extend([
        format!("      GROUP BY {key}, lo, hi"),
        "      HAVING lo <= hi".to_string(),
        "    )".to_string(),
        "    ARRAY JOIN [(lo, toInt64(m), toInt64(c)), (hi + 1, -toInt64(m), -toInt64(c))] AS d"
            .to_string(),
        format!("    GROUP BY {key}, k0"),
        "  )".to_string(),
        format!(
            "  WINDOW cum AS (PARTITION BY {key} ORDER BY k0 ASC \
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),"
        ),
        format!(
            "         whole AS (PARTITION BY {key} ORDER BY k0 ASC \
             ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
        ),
        ")".to_string(),
        format!("ARRAY JOIN range(k0, least(k1, {kend})) AS k"),
        "WHERE p > 0".to_string(),
    ]);
    let sql = lines.join("\n");
    Ok(sql)
}

/// The duration units the reduced statement decides, and each one's
/// nanoseconds (issue #624, part 4): Go's `time.ParseDuration` table.
const REDUCED_DURATION_UNITS: [(&str, u64); 8] = [
    ("ns", 1),
    ("us", 1_000),
    ("\u{b5}s", 1_000),
    ("\u{3bc}s", 1_000),
    ("ms", 1_000_000),
    ("s", 1_000_000_000),
    ("m", 60_000_000_000),
    ("h", 3_600_000_000_000),
];

/// The conversion columns of a lowered `| unwrap` (issue #624, part 4,
/// D6), over `ut`, the unwrapped label's stored text, in the order the
/// statement's `WITH` lists them: `us` is 0 for a value, 1 for a failure
/// and 2 for a text the statement does not decide; `uv` is the value.
///
/// Each conversion decides exactly the grammar written here, and its
/// value is today's route's (`pipeline::convert_label_value`) bit for bit;
/// a text outside it is undecided and fails the statement (395), which
/// sends the query to today's route. Rendered under `precise_float_parsing
/// = 1` ([`UNWRAP_PARSER_SETTING`]).
pub fn reduced_conversion_columns(conversion: UnwrapConversion) -> String {
    match conversion {
        UnwrapConversion::Number => {
            "match(ut, '^[+-]?([0-9]+[.]?[0-9]*|[.][0-9]+)([eE][+-]?[0-9]+)?$') AS ug, \
             toFloat64OrNull(ut) AS ux, \
             toUInt8(multiIf(ug AND ux IS NOT NULL AND isFinite(ux), 0, \
             NOT ug AND position(ut, '_') = 0 \
             AND NOT match(lower(ut), '^[+-]?(inf|infinity|nan)$|^[+-]?0x'), 1, 2)) AS us, \
             if(us = 0, assumeNotNull(ux), 0.) AS uv"
                .to_string()
        }
        UnwrapConversion::Duration => {
            let units: Vec<String> = REDUCED_DURATION_UNITS
                .iter()
                .map(|(u, _)| format!("'{u}'"))
                .collect();
            let (u, alt) = (
                format!("[{}]", units.join(", ")),
                REDUCED_DURATION_UNITS
                    .iter()
                    .map(|(u, _)| *u)
                    .collect::<Vec<_>>()
                    .join("|"),
            );
            let m = format!(
                "[{}]",
                REDUCED_DURATION_UNITS
                    .iter()
                    .map(|(_, n)| n.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            let p = format!(
                "[{}]",
                (0..=18)
                    .map(|i| format!("1e{i}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            format!(
                "match(ut, '^[+-]?(([0-9]{{1,18}}([.][0-9]{{0,18}})?|[.][0-9]{{1,18}})({alt}))+$') AS ug, \
                 arraySum(arrayMap(c -> toUInt128(toUInt64OrZero(c[1])) * toUInt128(transform(c[3], {u}, {m}, 0)) \
                 + toUInt128(if(toUInt64OrZero(c[2]) = 0, 0, \
                 toUInt64(toFloat64(toUInt64OrZero(c[2])) * (toFloat64(transform(c[3], {u}, {m}, 0)) / {p}[length(c[2]) + 1])))), \
                 extractAllGroups(ut, '([0-9]*)(?:[.]([0-9]*))?({alt})'))) AS ua, \
                 toUInt8(multiIf(match(ut, '^[+-]?0$'), 0, ug AND ua <= 9223372036854775807, 0, \
                 NOT match(ut, '[0-9]') OR NOT match(ut, '^[+-]?[0-9.]') \
                 OR match(ut, '^[+-]?([0-9]+[.]?[0-9]*|[.][0-9]+)$') \
                 OR (match(ut, '^[+-]?([0-9]+[.]?[0-9]*|[.][0-9]+)[^0-9.]+$') \
                 AND regexpExtract(ut, '([^0-9.]+)$', 1) NOT IN {u}), 1, 2)) AS us, \
                 if(us = 0 AND ug, if(startsWith(ut, '-'), -toInt64(ua), toInt64(ua)), toInt64(0)) AS ud, \
                 if(us = 0, toFloat64(intDiv(ud, toInt64(1000000000))) \
                 + toFloat64(ud % toInt64(1000000000)) / 1e9, 0.) AS uv"
            )
        }
        UnwrapConversion::Bytes => {
            let units = super::pipeline::BYTES_UNITS;
            let b = format!(
                "[{}]",
                units
                    .iter()
                    .map(|(u, _)| format!("'{u}'"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            let f = format!(
                "[{}]",
                units
                    .iter()
                    .map(|(_, f)| format!("{f:.1}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            let alt = units
                .iter()
                .filter(|(u, _)| !u.is_empty())
                .map(|(u, _)| *u)
                .collect::<Vec<_>>()
                .join("|");
            format!(
                "lower(ut) AS ul, \
                 match(ul, '^([0-9]+[.]?[0-9]*|[.][0-9]+) *({alt})? *$') AS ug, \
                 regexpExtract(ul, '^([0-9]+[.]?[0-9]*|[.][0-9]+)', 1) AS un, \
                 toFloat64OrNull(un) * transform(trimBoth(substring(ul, length(un) + 1)), {b}, {f}, 0.) AS up, \
                 toUInt8(multiIf(ug AND up IS NOT NULL AND up < 18446744073709551616., 0, \
                 NOT match(ut, '[0-9]') OR NOT match(ut, '^[0-9.,]') \
                 OR match(ul, '^([0-9]+[.]?[0-9]*|[.][0-9]+) *[a-z]+ *$'), 1, 2)) AS us, \
                 if(us = 0, trunc(assumeNotNull(up)), 0.) AS uv"
            )
        }
    }
}

/// The `WITH` columns of a lowered `| unwrap` from its parser to `e`
/// (issue #624, part 4b, §3.1-§3.3), over the stored line.
///
/// ```text
/// kd        the parser's readers decided the line (1 with no parser)
/// pk        the parser's key labels: (name, text) pairs, or one ('', fields)
///           pair for | logfmt; a name a stream label or metadata entry
///           shadows is left out in keep mode
/// ut, xk    x's text and its kind: 0 text, 1 JSON number, 2 failure,
///           3 undecided, 4 absent. A stream label x is read first, then
///           (with a parser) metadata x, which is undecided, then the parser
/// pu        the row is undecided: the statement throws on it (395)
/// st, v, e  the conversion's status and value; e = 1 for a failed value
/// ```
///
/// `conversion`'s columns are [`reduced_conversion_columns`] over `ut`; a
/// JSON number's are §3.3's, decided on its type.
pub fn reduced_parser_columns(
    value: &ReducedUnwrap,
    keys: &ReducedKeyColumns,
    conversion: UnwrapConversion,
) -> Result<String, KeyStatementRefusal> {
    use super::predicate::{
        ReaderColumns, json_depth_bound, json_flatten_key_budget_bound, literal,
        metadata_holds_name,
    };
    let keep = matches!(value.mode, KeyMode::Keep(_));
    let x = literal(&value.label);
    let x = x.as_sql().to_string();
    // A name of `pk` is left out where a stream label or, in keep mode, a
    // metadata entry of that name shadows the parser's value.
    let shown = |name: &str| -> String {
        let mut terms: Vec<String> = Vec::new();
        if let Some((_, fps)) = keys.pk_blank.iter().find(|(n, _)| n == name)
            && !fps.is_empty()
        {
            terms.push(format!("fingerprint NOT IN ({})", fp_list(fps)));
        }
        if keep {
            terms.push(format!("NOT {}", metadata_holds_name(name).as_sql()));
        }
        if terms.is_empty() {
            "1".to_string()
        } else {
            terms.join(" AND ")
        }
    };
    let empty_pk = "CAST([], 'Array(Tuple(String, String))')".to_string();
    let pairs = |items: Vec<String>| -> String {
        if items.is_empty() {
            empty_pk.clone()
        } else {
            format!(
                "arrayMap(p -> (p.2, p.3), arrayFilter(p -> p.1 = 1, [{}]))",
                items.join(", ")
            )
        }
    };
    let mut cols: Vec<String> = Vec::new();
    // The reader's text and kind for `x`, and a JSON number's (status, value).
    let (reader_ut, reader_xk, number): (String, String, Option<(String, String)>) = match &value
        .parser
    {
        None => {
            cols.push("toUInt8(1) AS kd".to_string());
            cols.push(format!("{empty_pk} AS pk"));
            (
                format!("JSONExtractString(structured_metadata, {x})"),
                "toUInt8(0)".to_string(),
                None,
            )
        }
        Some(ReducedParser::Json {
            form,
            keys: names,
            x_path,
        }) => {
            let mut kd: Vec<String> = Vec::new();
            let mut items: Vec<String> = Vec::new();
            for (i, key) in names.iter().enumerate() {
                let t = format!("l{i}");
                cols.extend(json_value_reader(
                    ReaderColumns::Key(i),
                    std::slice::from_ref(&key.source),
                    *form,
                )?);
                kd.push(format!("({t}_str + {t}_int + {t}_absent) > 0"));
                items.push(format!(
                    "(toUInt8({t}_str + {t}_int = 1 AND {}), {}, if({t}_str = 1, \
                         substring({t}_r, 2, length({t}_r) - 2), {t}_r))",
                    shown(&key.label),
                    literal(&key.source).as_sql()
                ));
            }
            cols.push(format!(
                "toUInt8({}) AS depth_ok",
                json_depth_bound().as_sql()
            ));
            let document = match form {
                UnwrapForm::Bare => {
                    cols.push(format!(
                        "toUInt8({}) AS keybudget_ok",
                        json_flatten_key_budget_bound().as_sql()
                    ));
                    cols.push(
                        "toUInt8(isValidJSON(body) AND JSONType(body) = 'Object') AS doc_ok"
                            .to_string(),
                    );
                    "depth_ok = 1 AND keybudget_ok = 1 AND doc_ok = 1"
                }
                UnwrapForm::Targeted => "depth_ok = 1 AND isValidJSON(body)",
            };
            kd.push(document.to_string());
            cols.push(format!("toUInt8({}) AS kd", kd.join(" AND ")));
            cols.push(format!("{} AS pk", pairs(items)));
            cols.extend(json_value_reader(ReaderColumns::X, x_path, *form)?);
            let segments: Vec<String> = x_path
                .iter()
                .map(|seg| literal(seg).as_sql().to_string())
                .collect();
            cols.push(format!("JSONType(body, {}) AS xt", segments.join(", ")));
            cols.push(format!(
                "toUInt8(multiIf(NOT ({document}) OR lx_amb = 1, 3, \
                     lx_absent = 1 OR lx_r = 'null', 4, lx_str = 1, 0, \
                     xt IN ('Int64', 'UInt64') AND lx_r = '0' AND position(body, '-0') > 0, 3, \
                     xt IN ('Int64', 'UInt64', 'Double'), 1, xt = 'Bool', 2, 3)) AS xr"
            ));
            let integer = "xt IN ('Int64', 'UInt64')";
            let number = match conversion {
                UnwrapConversion::Number => (
                    "if(toFloat64OrNull(lx_r) IS NULL, 2, 0)".to_string(),
                    "ifNull(toFloat64OrNull(lx_r), 0.)".to_string(),
                ),
                UnwrapConversion::Duration => (
                    format!("if({integer} AND lx_r = '0', 0, 1)"),
                    "0.".to_string(),
                ),
                UnwrapConversion::Bytes => (
                    format!(
                        "multiIf({integer} AND NOT startsWith(lx_r, '-'), \
                             if(toFloat64OrNull(lx_r) IS NULL, 2, 0), {integer}, 1, 2)"
                    ),
                    "ifNull(toFloat64OrNull(lx_r), 0.)".to_string(),
                ),
            };
            (
                "multiIf(xr = 0, substring(lx_r, 2, length(lx_r) - 2), xr = 4, '', lx_r)"
                    .to_string(),
                "xr".to_string(),
                Some(number),
            )
        }
        Some(ReducedParser::Logfmt {
            names,
            x_source,
            x_destination,
        }) => {
            let [decided, _] = logfmt_count_columns(&[]);
            cols.push(decided.replace(" AS decided", " AS kd"));
            cols.push(format!(
                "extractAll(body, {}) AS fs",
                literal(LOGFMT_FIELD).as_sql()
            ));
            let key_of = |f: &str| {
                format!(
                    "concat(if(substring({f}, 1, 1) BETWEEN '0' AND '9', '_', ''), \
                         translate(substringIndex({f}, '=', 1), {}, {}))",
                    literal(LOGFMT_KEY_PUNCTUATION).as_sql(),
                    literal(&"_".repeat(LOGFMT_KEY_PUNCTUATION.len())).as_sql()
                )
            };
            if names.is_empty() {
                cols.push(format!("{empty_pk} AS pk"));
            } else {
                let list: Vec<String> = names
                    .iter()
                    .map(|n| literal(n).as_sql().to_string())
                    .collect();
                cols.push(format!(
                    "[('', arrayStringConcat(arrayFilter(f -> has([{}], {}), fs), ' '))] AS pk",
                    list.join(", "),
                    key_of("f")
                ));
            }
            cols.push(format!(
                "arrayFilter(f -> {} = {}, fs) AS xf",
                key_of("f"),
                literal(x_source).as_sql()
            ));
            cols.push(
                    "if(position(xf[1], '=') > 0, substring(xf[1], position(xf[1], '=') + 1), '') AS xv"
                        .to_string(),
                );
            let destination = match x_destination {
                Some(d) if d != x_source => format!(
                    "arrayExists(f -> {} = {}, fs), 3, ",
                    key_of("f"),
                    literal(d).as_sql()
                ),
                _ => String::new(),
            };
            cols.push(format!(
                "toUInt8(multiIf(length(xf) = 0, 4, length(xf) >= 2, 3, xv = '', 4, \
                     startsWith(xv, '\"') AND position(xv, {}) > 0, 3, NOT isValidASCII(xv), 3, \
                     {destination}0)) AS xr",
                literal("\\").as_sql()
            ));
            (
                "if(startsWith(xv, '\"'), substring(xv, 2, length(xv) - 2), xv)".to_string(),
                "xr".to_string(),
                None,
            )
        }
        Some(ReducedParser::Capture {
            parser,
            pattern,
            groups,
            x_index,
        }) => {
            let extract = match parser {
                CaptureParser::Regexp => super::predicate::regexp_group_columns(pattern, &[]),
                CaptureParser::Pattern => super::predicate::pattern_group_columns(pattern, &[]),
            }
            .map_err(|_| {
                KeyStatementRefusal::Reader(
                    super::predicate::KeyReaderRefusal::PatternNotCompilable,
                )
            })?
            .extract;
            cols.push(format!("{} AS g", extract.as_sql()));
            cols.push("toUInt8(1) AS kd".to_string());
            let items: Vec<String> = groups
                .iter()
                .map(|(name, i)| {
                    format!(
                        "(toUInt8(length(g) >= {i} AND {}), {}, g[{i}])",
                        shown(name),
                        literal(name).as_sql()
                    )
                })
                .collect();
            cols.push(format!("{} AS pk", pairs(items)));
            (
                format!("if(length(g) >= {x_index}, g[{x_index}], '')"),
                format!("if(length(g) >= {x_index} AND g[{x_index}] != '', 0, 4)"),
                None,
            )
        }
    };
    // D3: a stream label `x` is read first; with a parser, metadata `x` is
    // the next source, and the statement does not decide it.
    let reader_xk = if value.parser.is_some() {
        format!(
            "if({}, 3, {reader_xk})",
            metadata_holds_name(&value.label).as_sql()
        )
    } else {
        reader_xk
    };
    let (ut, xk) = if keys.x_values.is_empty() {
        (reader_ut, reader_xk)
    } else {
        let fps: Vec<FpLiteral> = keys.x_values.iter().map(|(fp, _)| *fp).collect();
        let texts: Vec<String> = keys
            .x_values
            .iter()
            .map(|(_, t)| literal(t).as_sql().to_string())
            .collect();
        let carriers = fp_list(&fps);
        (
            format!(
                "if(fingerprint IN ({carriers}), transform(fingerprint, [{carriers}], [{}], ''), {reader_ut})",
                texts.join(", ")
            ),
            format!("if(fingerprint IN ({carriers}), 0, {reader_xk})"),
        )
    };
    cols.push(format!("{ut} AS ut"));
    cols.push(format!("toUInt8({xk}) AS xk"));
    cols.push(reduced_conversion_columns(conversion));
    cols.push("toUInt8(kd = 0 OR xk = 3) AS pu".to_string());
    let (number_status, number_value) = match &number {
        Some((status, value)) => (
            format!("xk = 1, {status}, "),
            format!("if(xk = 1, {value}, uv)"),
        ),
        None => (String::new(), "uv".to_string()),
    };
    cols.push(format!(
        "toUInt8(multiIf(position(structured_metadata, '\"__') > 0, 2, pu = 1, 2, xk = 2, 1, \
         {number_status}us)) AS st"
    ));
    cols.push(format!("{number_value} AS v"));
    cols.push("toUInt8(st = 1) AS e".to_string());
    Ok(cols.join(",\n         "))
}

/// The lowered `| unwrap` range statement (issue #624, part 4): one row per
/// `(class, metadata key, failed, grid point)` window that holds a sample,
/// its reducer computed in the database.
///
/// ```text
/// run grouping   one SELECT over the table: each line's key (c, kv, e) and the
///                grid indexes lo..hi whose windows hold it; per (key, lo, hi)
///                the run's partial state. Over a Distributed table each shard
///                runs this and sends its run states.
/// fan-out        each run to its grid points, merged per (key, k)
/// outer          the reducer over the window, in (timestamp, stream hash,
///                value) order where the order matters
/// ```
///
/// A line with no `x` (`ut = ''`) is no sample; a line no window holds
/// neither counts nor fails; a text the conversion does not decide, stored
/// metadata holding `"__`, or a line the parser's readers do not decide
/// (issue #624, part 4b) fails the statement (395). The parser runs in the
/// `SELECT` that reads the table ([`reduced_parser_columns`]), and its key
/// `pk` groups the runs beside `c`, `kv` and `e`.
///
/// # Refusals
///
/// Every refusal of [`super::predicate::sliding_cover`], and of
/// [`reduced_parser_columns`].
#[allow(clippy::too_many_arguments)]
pub fn metric_range_reduced(
    tenant: &Tenant,
    table: &str,
    value: &ReducedUnwrap,
    keys: &ReducedKeyColumns,
    services: &[CheckedLiteral],
    fingerprints: &[FpLiteral],
    scan: SlidingScan,
    extra_predicates: &[CheckedFragment],
    max_memory: Option<u64>,
) -> Result<String, KeyStatementRefusal> {
    use super::predicate::literal;
    use pulsus_logql::RangeAggOp as Op;
    let SlidingScan {
        window,
        lower,
        grid_start_ns,
        step_ns,
        range_ns,
    } = scan;
    let TimeWindow { start_ns, end_ns } = window;
    let cover =
        super::predicate::sliding_cover(start_ns, grid_start_ns, end_ns, step_ns, range_ns)?;
    let blank = |name: &str| -> &[FpLiteral] {
        keys.blank
            .iter()
            .find(|(n, _)| n == name)
            .map_or(&[][..], |(_, fps)| fps.as_slice())
    };
    let kv = match &value.mode {
        KeyMode::Keep(names) if names.is_empty() => {
            "CAST([], 'Array(Tuple(String, String))')".to_string()
        }
        KeyMode::Keep(names) => {
            let mut names = names.clone();
            names.sort_unstable();
            let items: Vec<String> = names
                .iter()
                .map(|n| {
                    let lit = literal(n);
                    let read = format!("JSONExtractString(structured_metadata, {})", lit.as_sql());
                    let fps = blank(n);
                    let read = if fps.is_empty() {
                        read
                    } else {
                        format!("if(fingerprint IN ({}), '', {read})", fp_list(fps))
                    };
                    format!("({}, {read})", lit.as_sql())
                })
                .collect();
            format!("[{}]", items.join(", "))
        }
        KeyMode::Drop(names) => {
            let terms: Vec<String> = names
                .iter()
                .map(|n| {
                    let fps = blank(n);
                    let lit = literal(n);
                    if fps.is_empty() {
                        format!("p.1 = {}", lit.as_sql())
                    } else {
                        format!(
                            "(p.1 = {} AND fingerprint NOT IN ({}))",
                            lit.as_sql(),
                            fp_list(fps)
                        )
                    }
                })
                .collect();
            format!(
                "arrayFilter(p -> NOT ({}), JSONExtractKeysAndValues(structured_metadata, 'String'))",
                terms.join(" OR ")
            )
        }
    };
    let has_h = !keys.hashes.is_empty();
    let h = if has_h {
        let fps: Vec<FpLiteral> = keys.hashes.iter().map(|(fp, _)| *fp).collect();
        let hashes: Vec<String> = keys.hashes.iter().map(|(_, h)| h.to_string()).collect();
        format!(
            ",\n         transform(fingerprint, [{}], [{}], toUInt64(0)) AS h",
            fp_list(&fps),
            hashes.join(", ")
        )
    } else {
        String::new()
    };
    let sorted = if has_h {
        "arraySort((x, a, b) -> (a, b, x), wv, wt, wh)"
    } else {
        "arraySort((x, a) -> (a, x), wv, wt)"
    };
    let (run, merge) = match value.reducer {
        Op::QuantileOverTime => (
            "count() AS rn, groupArray(v) AS rv".to_string(),
            "sum(rn) AS n, groupArrayArray(rv) AS wv".to_string(),
        ),
        Op::MinOverTime | Op::MaxOverTime => {
            let f = if value.reducer == Op::MinOverTime {
                "min"
            } else {
                "max"
            };
            (
                format!("count() AS rn, {f}(v) AS rm"),
                format!("sum(rn) AS n, {f}(rm) AS m"),
            )
        }
        Op::FirstOverTime | Op::LastOverTime => {
            let f = if value.reducer == Op::FirstOverTime {
                "min"
            } else {
                "max"
            };
            let tuple = if has_h { "(t, h, v)" } else { "(t, v)" };
            (
                format!("count() AS rn, {f}({tuple}) AS rm"),
                format!("sum(rn) AS n, {f}(rm) AS m"),
            )
        }
        _ => {
            let (rh, wh) = if has_h {
                (" groupArray(h) AS rh,", " groupArrayArray(rh) AS wh,")
            } else {
                ("", "")
            };
            (
                format!("count() AS rn, groupArray(t) AS rt,{rh} groupArray(v) AS rv"),
                format!("sum(rn) AS n, groupArrayArray(rt) AS wt,{wh} groupArrayArray(rv) AS wv"),
            )
        }
    };
    let zero = ("toInt64(0)", "toInt64(0)", "toFloat64(0)");
    let (r, t1, t2, v1) = match value.reducer {
        Op::SumOverTime | Op::Rate => {
            (format!("arrayCumSum({sorted})[-1]"), zero.0, zero.1, zero.2)
        }
        Op::AvgOverTime => (
            format!(
                "arrayFold((acc, ch) -> arrayFold((a, x) -> (a.1 + 1, if(isInfinite(a.2) \
                 AND ((isInfinite(x) AND ((a.2 > 0) = (x > 0))) OR isFinite(x)), a.2, \
                 a.2 + (x / (a.1 + 1) - a.2 / (a.1 + 1)))), ch, acc), \
                 arraySplit((x, i) -> i % 1024 = 1, ({sorted} AS s), arrayEnumerate(s)), \
                 (toFloat64(0), toFloat64(0))).2"
            ),
            zero.0,
            zero.1,
            zero.2,
        ),
        Op::StdvarOverTime | Op::StddevOverTime => (
            format!("arrayReduce('varPopStable', {sorted})"),
            zero.0,
            zero.1,
            zero.2,
        ),
        Op::RateCounter => (
            format!(
                "arrayCumSum(arrayConcat([({sorted} AS s)[-1] - s[1]], \
                 arrayFilter((p, x) -> x < p, arrayPushFront(arrayPopBack(s), 0.), s)))[-1]"
            ),
            "arrayMin(wt)",
            "arrayMax(wt)",
            "s[1]",
        ),
        Op::QuantileOverTime => {
            let q = value.quantile.unwrap_or(0.0).clamp(0.0, 1.0);
            (
                format!(
                    "(arraySort(wv) AS s)[(toUInt64(floor(reinterpretAsFloat64(toUInt64({})) \
                     * (length(s) - 1) AS rk)) AS lw) + 1] * (1 - (rk - floor(rk))) \
                     + s[toUInt64(ceil(rk)) + 1] * (rk - floor(rk))",
                    q.to_bits()
                ),
                zero.0,
                zero.1,
                zero.2,
            )
        }
        Op::MinOverTime | Op::MaxOverTime => ("m".to_string(), zero.0, zero.1, zero.2),
        Op::FirstOverTime | Op::LastOverTime => (
            if has_h { "m.3" } else { "m.2" }.to_string(),
            zero.0,
            zero.1,
            zero.2,
        ),
        Op::CountOverTime | Op::BytesOverTime | Op::BytesRate | Op::AbsentOverTime => {
            return Err(super::predicate::BucketGridRefusal::RollupSource.into());
        }
    };
    let columns = reduced_parser_columns(value, keys, value.conversion)?;
    let class = unwrapped_class_expr(&keys.classes);
    let (lo, hi) = (cover.lo.as_sql(), cover.hi.as_sql());
    let lower_op = lower.sql_op();
    let mut lines: Vec<String> = vec![
        format!(
            "SELECT c, kv, pk, e, toInt64({grid_start_ns} + k * {step_ns}) AS bucket_ns, n, {r} AS r, \
             {t1} AS t1, {t2} AS t2, {v1} AS v1"
        ),
        "FROM (".to_string(),
        format!("  SELECT c, kv, pk, e, k, {merge}"),
        "  FROM (".to_string(),
        format!("    WITH {class} AS c, {kv} AS kv,"),
        format!("         {lo} AS lo, {hi} AS hi, timestamp_ns AS t,"),
        format!("         {columns}{h}"),
        format!("    SELECT c, kv, pk, e, lo, hi, {run}"),
        format!("    FROM {table}"),
    ];
    if !services.is_empty() {
        lines.push(format!("    PREWHERE {}", service_predicate(services)));
    }
    lines.push(format!(
        "    WHERE org_id = {} AND fingerprint IN ({}) AND timestamp_ns {lower_op} {start_ns} AND timestamp_ns <= {end_ns}",
        tenant.sql_literal(),
        fp_list(fingerprints)
    ));
    for clause in extra_predicates {
        lines.push(format!("      AND {}", clause.as_sql()));
    }
    lines.extend([
        // One comparison: the engine evaluates a separate `ut != ''`
        // conjunct first, and would never reach the throw on an undecided
        // parser row with no text for `x` (issue #624, part 4b, §3.1).
        "      AND throwIf(st = 2 AND (ut != '' OR pu = 1) AND lo <= hi) + toUInt8(ut != '' AND lo <= hi) = 1"
            .to_string(),
        "    GROUP BY c, kv, pk, e, lo, hi".to_string(),
        "  )".to_string(),
        "  ARRAY JOIN range(lo, hi + 1) AS k".to_string(),
        "  GROUP BY c, kv, pk, e, k".to_string(),
        ")".to_string(),
        "ORDER BY c, kv, pk, e, bucket_ns".to_string(),
    ]);
    let mut settings =
        format!("SETTINGS {UNWRAP_PARSER_SETTING}, query_plan_execute_functions_after_sorting = 0");
    // The `| logfmt` form's `WITH` aliases expand past the server's default
    // syntax-tree limit (issue #624, part 4b: `TOO_BIG_AST` without these).
    if matches!(value.parser, Some(ReducedParser::Logfmt { .. })) {
        settings.push_str(", max_ast_elements = 1000000, max_expanded_ast_elements = 10000000");
    }
    if let Some(n) = max_memory {
        settings.push_str(&format!(", max_memory_usage = {n}"));
    }
    lines.push(settings);
    Ok(lines.join("\n"))
}

fn unwrapped_class_expr(classes: &Option<Vec<Vec<FpLiteral>>>) -> String {
    match classes {
        None => "fingerprint".to_string(),
        Some(classes) if classes.len() <= 1 => "toUInt128(0)".to_string(),
        Some(classes) => {
            let fps: Vec<FpLiteral> = classes.iter().flatten().copied().collect();
            // The class ids are small ordinals, NOT fingerprints, and they
            // are rendered as the `UInt64`s they are. The `transform` is
            // then widened to `UInt128` so that `class` is one type across
            // every arm of this function — the no-grouping arm above
            // renders the `fingerprint` column itself, and a row struct
            // decodes one type per column.
            //
            // **`transform` cannot RETURN a `UInt128`** (issue #498):
            // ClickHouse 26.3.29.7 answers `Unexpected type UInt128 in
            // function 'transform'` when the `to` array or the default is
            // one, while accepting a `UInt128` as the value being mapped.
            // So the widening is outside the call rather than inside its
            // arguments.
            let ids: Vec<String> = classes
                .iter()
                .enumerate()
                .flat_map(|(id, c)| std::iter::repeat_n(id.to_string(), c.len()))
                .collect();
            format!(
                "toUInt128(transform(fingerprint, [{}], [{}], toUInt64(0)))",
                fp_list(&fps),
                ids.join(", ")
            )
        }
    }
}

/// Renders the metric-read `PREWHERE service ...\n` line, or an empty
/// string when `services` is empty (the rollup path — no `service` column
/// to filter on).
/// `body, ` when the reducer reads the line's length, so the extraction
/// level under a `regexp` count carries it (issue #624, part 3a); empty for
/// `count()`.
fn regexp_body_column(agg_expr: &str) -> &'static str {
    if agg_expr.contains("body") {
        "body, "
    } else {
        ""
    }
}

/// The three extraction columns of a `regexp` count (issue #624, part 3a,
/// D2), each line indented by `indent`.
fn regexp_extraction_lines(
    regexp: &super::predicate::RegexpGroupColumns,
    indent: &str,
) -> [String; 3] {
    [
        format!("{indent}{} AS g,", regexp.extract.as_sql()),
        format!("{indent}toUInt8(length(g) > 0) AS matched,"),
        format!("{indent}{} AS caps", regexp.caps.as_sql()),
    ]
}

fn metric_prewhere(services: &[CheckedLiteral]) -> String {
    if services.is_empty() {
        String::new()
    } else {
        format!("PREWHERE {}\n", service_predicate(services))
    }
}

/// The comma-separated `toUInt128('<decimal>')` list every stage-2 and
/// stage-3 `fingerprint IN (...)` carries.
///
/// The call form is not optional above `2^64`: ClickHouse reads a bare
/// decimal literal there as `Float64`, exact only to `2^53`, so a bare
/// list silently matches a neighbouring fingerprint under `=` and prunes
/// every granule under `IN` (issue #498, measured on ClickHouse
/// 26.3.29.7). The parameter is a minted literal, never a `Fingerprint`,
/// so the form is the type's rather than this function's.
pub(in crate::logql) fn fp_list(fingerprints: &[FpLiteral]) -> String {
    fingerprints
        .iter()
        .map(FpLiteral::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// The singleton-equality/`IN` split shared by every stage 3 style
/// predicate over a resolved value set (architect plan amendment §2).
fn service_predicate(services: &[CheckedLiteral]) -> String {
    if services.len() == 1 {
        format!("service = {}", services[0].as_sql())
    } else {
        format!(
            "service IN ({})",
            services
                .iter()
                .map(CheckedLiteral::as_sql)
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

/// The rollup range metric read (issue #614, part 2): the sliding
/// statement's grid arithmetic over `log_metrics_<res>` instead of lines.
///
/// Each rollup row splits into two weighted points: the lines exactly on
/// its bucket start `b` (`count_at`, `bytes_at`) at `b`, and the rest of
/// the bucket at `b + res`. With the grid start, step and range multiples
/// of `res`, every window edge is a bucket start, and a line in `(b, b +
/// res)` is in window `(t - range, t]` exactly when a line at `b + res`
/// would be — so [`super::predicate::sliding_cover`] and the window
/// arithmetic of [`metric_range_sliding`] apply unchanged, over the point
/// the `ARRAY JOIN` names `timestamp_ns`.
///
/// No `FINAL`: the rollup's unmerged rows are partial sums, and every
/// column the statement reads is summed.
///
/// `structured_metadata` is the empty string on every row: the rollup does
/// not store it, and the planner routes here only a query whose answer
/// cannot depend on it.
///
/// # Refusals
///
/// `OffTheRollupGrid` for a shape that is not a rollup shape, a resolution
/// that is not positive, a grid start, step or range that is not a multiple
/// of it, or an inclusive scan bound; `WouldOverflow` when `end + res` is not
/// representable; and every refusal of [`super::predicate::sliding_cover`].
pub fn metric_range_rollup(
    tenant: &Tenant,
    source: MetricSource<'_>,
    fingerprints: &[FpLiteral],
    scan: SlidingScan,
    res_ns: i64,
) -> Result<String, super::predicate::BucketGridRefusal> {
    use super::predicate::BucketGridRefusal;
    let t = tenant.sql_literal();
    let MetricSource { table, shape } = source;
    let m = match shape {
        MetricShape::RollupCount => "sum(lines)",
        MetricShape::RollupBytes => "sum(line_bytes)",
        MetricShape::RawCount | MetricShape::RawBytes => {
            return Err(BucketGridRefusal::OffTheRollupGrid);
        }
    };
    let SlidingScan {
        window,
        lower,
        grid_start_ns,
        step_ns,
        range_ns,
    } = scan;
    let TimeWindow { start_ns, end_ns } = window;
    if res_ns <= 0
        || grid_start_ns.rem_euclid(res_ns) != 0
        || step_ns.rem_euclid(res_ns) != 0
        || range_ns.rem_euclid(res_ns) != 0
        || lower != ScanLowerBound::Exclusive
    {
        return Err(BucketGridRefusal::OffTheRollupGrid);
    }
    end_ns
        .checked_add(res_ns)
        .ok_or(BucketGridRefusal::WouldOverflow)?;
    let cover =
        super::predicate::sliding_cover(start_ns, grid_start_ns, end_ns, step_ns, range_ns)?;
    let (lo, hi, kend) = (cover.lo.as_sql(), cover.hi.as_sql(), cover.kend);
    let fp_list = fp_list(fingerprints);
    let lines: Vec<String> = vec![
        format!(
            "SELECT fingerprint, toInt64({grid_start_ns} + k * {step_ns}) AS bucket_ns, \
             toUInt64(v) AS n, '' AS structured_metadata"
        ),
        "FROM (".to_string(),
        "  SELECT fingerprint, k0,".to_string(),
        format!(
            "         sum(dv) OVER cum AS v, sum(dc) OVER cum AS p, \
             leadInFrame(k0, 1, {kend}) OVER whole AS k1"
        ),
        "  FROM (".to_string(),
        "    SELECT fingerprint, d.1 AS k0, sum(d.2) AS dv, sum(d.3) AS dc".to_string(),
        "    FROM (".to_string(),
        "      SELECT fingerprint,".to_string(),
        format!("             {lo} AS lo,"),
        format!("             {hi} AS hi,"),
        format!("             {m} AS m, sum(lines) AS c"),
        format!("      FROM {table}"),
        format!("      ARRAY JOIN [bucket_ns + {res_ns}, bucket_ns] AS timestamp_ns,"),
        "                 [toInt64(count) - toInt64(count_at), toInt64(count_at)] AS lines,"
            .to_string(),
        "                 [toInt64(bytes) - toInt64(bytes_at), toInt64(bytes_at)] AS line_bytes"
            .to_string(),
        format!("      WHERE org_id = {t} AND fingerprint IN ({fp_list})"),
        format!("        AND bucket_ns >= {start_ns} AND bucket_ns <= {end_ns}"),
        format!("        AND lines > 0 AND timestamp_ns > {start_ns} AND timestamp_ns <= {end_ns}"),
        "      GROUP BY fingerprint, lo, hi".to_string(),
        "      HAVING lo <= hi".to_string(),
        "    )".to_string(),
        "    ARRAY JOIN [(lo, toInt64(m), toInt64(c)), (hi + 1, -toInt64(m), -toInt64(c))] AS d"
            .to_string(),
        "    GROUP BY fingerprint, k0".to_string(),
        "  )".to_string(),
        "  WINDOW cum AS (PARTITION BY fingerprint ORDER BY k0 ASC \
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),"
            .to_string(),
        "         whole AS (PARTITION BY fingerprint ORDER BY k0 ASC \
         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
            .to_string(),
        ")".to_string(),
        format!("ARRAY JOIN range(k0, least(k1, {kend})) AS k"),
        "WHERE p > 0".to_string(),
    ];
    Ok(lines.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logql::predicate::{literal, month_literal};
    use pulsus_model::Fingerprint;

    /// The pushed-down fragment fixture these builders' loop tests append.
    ///
    /// Issue #286 replaced the previous hand-written
    /// `positionCaseSensitive(body, 'err') > 0` string with a genuinely
    /// minted one: [`CheckedFragment`] has no constructor outside
    /// `logql::predicate`, and no mint renders `positionCaseSensitive`, so
    /// the old fixture is not expressible. `|= "err"` renders
    /// `body LIKE '%err%'` (issue #450), and the three
    /// expectations below carry that text instead. The property under test
    /// — the builder appends each fragment behind its own `AND` — is
    /// unchanged.
    fn err_line_filter() -> CheckedFragment {
        crate::logql::predicate::line_filter(&pulsus_logql::LineFilter {
            op: pulsus_logql::LineFilterOp::Contains,
            value: "err".to_string(),
            value_is_ip: false,
            or_matches: Vec::new(),
        })
        .expect("a Contains filter compiles no regex")
    }

    // -----------------------------------------------------------------
    // Issue #286: `MetricShape` — single-sourced, wildcard-free, total.
    // -----------------------------------------------------------------

    /// AC13(b): the inverse is derived from [`MetricShape::ALL`], which the
    /// declaring `metric_shapes!` invocation emits — so a fifth shape is
    /// covered here without this test being edited.
    #[test]
    fn metric_shape_round_trips_through_from_columns_for_every_variant() {
        let mut covered = 0usize;
        for &shape in MetricShape::ALL {
            assert_eq!(
                MetricShape::from_columns(shape.bucket_col(), shape.agg_expr()),
                Some(shape),
                "{shape:?} must round-trip"
            );
            covered += 1;
        }
        // Derived from the enum, NOT hand-listed: `ALL` is emitted by the
        // same `metric_shapes!` invocation that declares the variants, so a
        // shape added there is covered here without this test being edited
        // (issue #286 AC16 — demonstrated by adding a fifth line and
        // watching `covered` become 5).
        assert_eq!(covered, MetricShape::ALL.len());
        println!("metric_shape round-trip covered {covered} shapes");
    }

    /// AC13(c): caller text cannot masquerade as a column pair.
    #[test]
    fn metric_shape_from_columns_refuses_a_foreign_pair() {
        assert_eq!(
            MetricShape::from_columns("timestamp_ns", "match(body, '(')"),
            None
        );
        assert_eq!(MetricShape::from_columns("bucket_ns", "count()"), None);
        assert_eq!(MetricShape::from_columns("", ""), None);
    }

    /// AC13(d): the four pairs are, verbatim, the literals `plan.rs`'s
    /// `metric_plan` spelled by hand before issue #286 sealed them.
    #[test]
    fn metric_shape_columns_are_the_pre_286_literals_verbatim() {
        assert_eq!(MetricShape::rollup(false), MetricShape::RollupCount);
        assert_eq!(MetricShape::rollup(true), MetricShape::RollupBytes);
        assert_eq!(MetricShape::raw(false), MetricShape::RawCount);
        assert_eq!(MetricShape::raw(true), MetricShape::RawBytes);
        assert_eq!(
            (
                MetricShape::RollupCount.bucket_col(),
                MetricShape::RollupCount.agg_expr()
            ),
            ("bucket_ns", "sum(count)")
        );
        assert_eq!(
            (
                MetricShape::RollupBytes.bucket_col(),
                MetricShape::RollupBytes.agg_expr()
            ),
            ("bucket_ns", "sum(bytes)")
        );
        assert_eq!(
            (
                MetricShape::RawCount.bucket_col(),
                MetricShape::RawCount.agg_expr()
            ),
            ("timestamp_ns", "count()")
        );
        assert_eq!(
            (
                MetricShape::RawBytes.bucket_col(),
                MetricShape::RawBytes.agg_expr()
            ),
            ("timestamp_ns", "sum(length(body))")
        );
    }

    /// The sealed source hands back exactly what it was built from.
    #[test]
    fn metric_source_new_is_the_only_way_in_and_reads_back_its_shape() {
        let s = MetricSource::new("log_metrics_5s", MetricShape::RollupBytes);
        assert_eq!(s.table(), "log_metrics_5s");
        assert_eq!(s.shape(), MetricShape::RollupBytes);
        assert_eq!(s.shape().bucket_col(), "bucket_ns");
        assert_eq!(s.shape().agg_expr(), "sum(bytes)");
    }

    #[test]
    fn stage2_renders_the_canonical_hydration_shape() {
        assert_eq!(
            stage2(
                &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                "log_streams",
                &[
                    Fingerprint::from_raw(18374).sql_literal(),
                    Fingerprint::from_raw(99120).sql_literal()
                ]
            ),
            "SELECT fingerprint, service, labels FROM log_streams WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))"
        );
    }

    /// The `[start, end]` window every discovery-builder test below
    /// renders — 5s-aligned so the floored lower bound equals `start_ns`
    /// and the expectation stays readable.
    const DISCOVERY_WINDOW: TimeWindow = TimeWindow {
        start_ns: 1_751_328_000_000_000_000,
        end_ns: 1_751_331_600_000_000_000,
    };
    const RES_5S: u64 = 5_000_000_000;
    const ACTIVE_ALL: &str = "SELECT DISTINCT fingerprint FROM log_metrics_5s WHERE org_id = '' AND bucket_ns >= 1751328000000000000 AND bucket_ns <= 1751331600000000000";

    /// The active-fingerprint set the caller resolved and renders into a
    /// discovery scan (issue #603): a literal list, never a subquery.
    fn active_two() -> Vec<FpLiteral> {
        vec![
            Fingerprint::from_raw(7).sql_literal(),
            Fingerprint::from_raw(9).sql_literal(),
        ]
    }
    const ACTIVE_TWO_LIST: &str = "toUInt128('7'), toUInt128('9')";

    #[test]
    fn label_names_renders_a_distinct_key_scan_for_one_month() {
        assert_eq!(
            label_names(
                &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                "log_streams_idx",
                &[month_literal(2026, 7)],
                &active_two()
            ),
            format!(
                "SELECT DISTINCT key AS name\nFROM log_streams_idx\nWHERE org_id = '' AND month = '2026-07-01'\n  AND fingerprint IN ({ACTIVE_TWO_LIST})\nORDER BY name"
            )
        );
    }

    #[test]
    fn label_names_renders_a_month_in_list_for_a_boundary_spanning_window() {
        let sql = label_names(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            "log_streams_idx",
            &[month_literal(2026, 7), month_literal(2026, 8)],
            &active_two(),
        );
        assert!(sql.contains("WHERE org_id = '' AND month IN ('2026-07-01', '2026-08-01')"));
    }

    #[test]
    fn label_values_renders_a_distinct_value_scan_scoped_to_one_key() {
        assert_eq!(
            label_values(
                &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                "log_streams_idx",
                &[month_literal(2026, 7)],
                &literal("env"),
                &active_two()
            ),
            format!(
                "SELECT DISTINCT val AS value\nFROM log_streams_idx\nWHERE org_id = '' AND month = '2026-07-01' AND key = 'env'\n  AND fingerprint IN ({ACTIVE_TWO_LIST})\nORDER BY value"
            )
        );
    }

    #[test]
    fn detected_labels_renders_one_row_per_key_with_the_id_predicate() {
        let sql = detected_labels(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            "log_streams_idx",
            &[month_literal(2026, 7)],
            &active_two(),
        );
        assert!(sql.starts_with("SELECT key, uniqExact(val) AS cardinality, countIf(toFloat64OrNull(val) IS NULL AND NOT match(val, "));
        assert!(sql.contains("WHERE org_id = '' AND month = '2026-07-01'"));
        assert!(sql.contains(&format!("\n  AND fingerprint IN ({ACTIVE_TWO_LIST})\n")));
        assert!(sql.ends_with("GROUP BY key\nORDER BY key"));
    }

    /// **Issue #603: no discovery builder nests a subquery.** The activity
    /// scan is a statement of its own, so each of the three renders exactly
    /// one `SELECT` and names only the index table — which is what decides
    /// which route the server takes on a clustered deployment.
    #[test]
    fn no_discovery_builder_nests_a_subquery() {
        let months = [month_literal(2026, 7)];
        for (name, sql) in [
            (
                "label_names",
                label_names(
                    &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                    "log_streams_idx",
                    &months,
                    &active_two(),
                ),
            ),
            (
                "label_values",
                label_values(
                    &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                    "log_streams_idx",
                    &months,
                    &literal("env"),
                    &active_two(),
                ),
            ),
            (
                "detected_labels",
                detected_labels(
                    &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                    "log_streams_idx",
                    &months,
                    &active_two(),
                ),
            ),
        ] {
            assert_eq!(
                sql.matches("SELECT").count(),
                1,
                "{name} must nest no second SELECT: {sql}"
            );
            assert!(
                !sql.contains("log_metrics_5s"),
                "{name} must name only the index table: {sql}"
            );
        }
    }

    /// Issue #399 AC2 — the discriminator against the plausible-but-wrong
    /// `bucket_ns > start_ns` copy from [`log_stats_rollup`]: the bound is
    /// the bucket CONTAINING `start_ns`.
    #[test]
    fn activity_lower_bucket_ns_floors_to_the_containing_bucket() {
        const B: i64 = 100 * RES_5S as i64;
        assert_eq!(activity_lower_bucket_ns(B + 1_000_000_000, RES_5S), B);
        assert_eq!(activity_lower_bucket_ns(B, RES_5S), B);
        assert_eq!(activity_lower_bucket_ns(B - 1, RES_5S), B - RES_5S as i64);
        // Floor, not truncation-toward-zero: `-1 / 5e9` is `0` in Rust's
        // integer division and would round a pre-epoch bound UP.
        assert_eq!(activity_lower_bucket_ns(-1, RES_5S), -(RES_5S as i64));
        // No input can overflow or panic.
        assert_eq!(
            activity_lower_bucket_ns(i64::MIN, u64::MAX),
            i64::MIN.div_euclid(i64::MAX).saturating_mul(i64::MAX)
        );
        // A zero resolution is not representable in config, but the
        // builder must not divide by zero if one ever reaches it.
        assert_eq!(activity_lower_bucket_ns(7, 0), 7);
    }

    #[test]
    fn active_fingerprints_renders_a_streaming_distinct_over_the_rollup_pk() {
        assert_eq!(
            active_fingerprints(
                &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                "log_metrics_5s",
                None,
                DISCOVERY_WINDOW,
                RES_5S
            ),
            ACTIVE_ALL
        );
        assert_eq!(
            active_fingerprints(
                &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                "log_metrics_5s",
                Some(&[
                    Fingerprint::from_raw(101).sql_literal(),
                    Fingerprint::from_raw(205).sql_literal()
                ]),
                DISCOVERY_WINDOW,
                RES_5S
            ),
            "SELECT DISTINCT fingerprint FROM log_metrics_5s WHERE org_id = '' AND fingerprint IN (toUInt128('101'), toUInt128('205')) \
             AND bucket_ns >= 1751328000000000000 AND bucket_ns <= 1751331600000000000"
        );
    }

    /// A `start_ns` one nanosecond past a bucket edge must render the
    /// bucket's own start, not `start_ns` — the whole point of #399's
    /// floor (a sample at `B + 1s` lives in bucket `B`).
    #[test]
    fn active_fingerprints_lower_bound_is_the_containing_bucket_not_the_request_start() {
        let b = 1_751_328_000_000_000_000_i64;
        let sql = active_fingerprints(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            "log_metrics_5s",
            None,
            TimeWindow {
                start_ns: b + 1_000_000_000,
                end_ns: b + 4_000_000_000,
            },
            RES_5S,
        );
        assert!(
            sql.contains(&format!("bucket_ns >= {b} AND")),
            "lower bound must floor to the containing bucket: {sql}"
        );
    }

    #[test]
    fn service_predicate_is_bare_equality_for_one_service() {
        assert_eq!(
            service_predicate(&[literal("checkout")]),
            "service = 'checkout'"
        );
    }

    #[test]
    fn service_predicate_is_in_list_for_multiple_services() {
        assert_eq!(
            service_predicate(&[literal("checkout"), literal("billing")]),
            "service IN ('checkout', 'billing')"
        );
    }

    #[test]
    fn fp_list_joins_with_comma_space() {
        let fps = [1, 2, 3].map(|v| Fingerprint::from_raw(v).sql_literal());
        assert_eq!(
            fp_list(&fps),
            "toUInt128('1'), toUInt128('2'), toUInt128('3')"
        );
    }

    /// The four values where a bare decimal and the exact call form first
    /// disagree (issue #498): `2^64-1` is the last value a bare literal
    /// reads exactly, `2^64` is the first `Float64` and is what `2^64+1`
    /// rounds ONTO, and `2^64+2` is its neighbour — a test built only on
    /// the first two passes on a build that renders bare decimals.
    #[test]
    fn fp_list_renders_the_exact_call_form_at_the_2_64_boundary() {
        let fps = [
            18_446_744_073_709_551_615u128,
            18_446_744_073_709_551_616,
            18_446_744_073_709_551_617,
            18_446_744_073_709_551_618,
        ]
        .map(|v| Fingerprint::from_raw(v).sql_literal());
        assert_eq!(
            fp_list(&fps),
            "toUInt128('18446744073709551615'), toUInt128('18446744073709551616'), \
             toUInt128('18446744073709551617'), toUInt128('18446744073709551618')"
        );
    }

    #[test]
    fn metric_range_omits_prewhere_when_services_is_empty_the_rollup_path() {
        let sql = metric_range(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            MetricSource::new("log_metrics_5s", MetricShape::RollupCount),
            &[],
            &[
                Fingerprint::from_raw(1).sql_literal(),
                Fingerprint::from_raw(2).sql_literal(),
            ],
            TimeWindow {
                start_ns: 0,
                end_ns: 100,
            },
            ScanLowerBound::Exclusive,
            60,
            &[],
        );
        assert!(!sql.contains("PREWHERE"));
    }

    #[test]
    fn metric_range_renders_singleton_prewhere_for_the_raw_fallback() {
        let sql = metric_range(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            MetricSource::new("log_samples", MetricShape::RawCount),
            &[literal("checkout")],
            &[
                Fingerprint::from_raw(1).sql_literal(),
                Fingerprint::from_raw(2).sql_literal(),
            ],
            TimeWindow {
                start_ns: 0,
                end_ns: 100,
            },
            ScanLowerBound::Exclusive,
            60,
            &[],
        );
        assert!(sql.contains("PREWHERE service = 'checkout'\n"));
    }

    #[test]
    fn metric_range_renders_in_list_prewhere_for_multiple_services() {
        let sql = metric_range(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            MetricSource::new("log_samples", MetricShape::RawCount),
            &[literal("checkout"), literal("billing")],
            &[
                Fingerprint::from_raw(1).sql_literal(),
                Fingerprint::from_raw(2).sql_literal(),
            ],
            TimeWindow {
                start_ns: 0,
                end_ns: 100,
            },
            ScanLowerBound::Exclusive,
            60,
            &[],
        );
        assert!(sql.contains("PREWHERE service IN ('checkout', 'billing')\n"));
    }

    /// Issue #74 first-bound AC (hermetic half): the first tail page
    /// carries the explicit exclusive API `start` bound — the repo
    /// stage-3 convention — and NO keyset term, byte-exact.
    #[test]
    fn stage3_keyset_first_page_is_byte_exact_with_the_exclusive_start_bound() {
        let sql = stage3_keyset(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            "log_samples",
            &[literal("checkout")],
            &[Fingerprint::from_raw(18374).sql_literal()],
            TimeWindow {
                start_ns: 1_000,
                end_ns: 2_000,
            },
            KeysetLower::First,
            Direction::Forward,
            &[],
            500,
        );
        assert_eq!(
            sql,
            "SELECT fingerprint, timestamp_ns, body, cityHash64(body) AS body_hash, structured_metadata\n\
             FROM log_samples\n\
             PREWHERE service = 'checkout'\n\
             WHERE org_id = '' AND fingerprint IN (toUInt128('18374'))\n\
             \x20 AND timestamp_ns > 1000 AND timestamp_ns <= 2000\n\
             ORDER BY timestamp_ns ASC, fingerprint ASC, body_hash ASC, body ASC\n\
             LIMIT 500"
        );
    }

    /// Issue #74 (round-4 adjudication #1): the keyset page — inclusive
    /// `>=` composite predicate, redundant time lower bound for granule
    /// pruning, total ORDER BY with the raw-`body` tiebreaker, and the
    /// server-side occurrence-count `OFFSET` — byte-exact.
    #[test]
    fn stage3_keyset_later_page_is_byte_exact_with_inclusive_tuple_and_offset() {
        let sql = stage3_keyset(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            "log_samples",
            &[literal("checkout"), literal("billing")],
            &[
                Fingerprint::from_raw(1).sql_literal(),
                Fingerprint::from_raw(2).sql_literal(),
            ],
            TimeWindow {
                start_ns: 1_000,
                end_ns: 2_000,
            },
            KeysetLower::After {
                tuple: (1_500, Fingerprint::from_raw(7).sql_literal(), 42),
                offset: 3,
            },
            Direction::Forward,
            &[err_line_filter()],
            500,
        );
        assert_eq!(
            sql,
            "SELECT fingerprint, timestamp_ns, body, cityHash64(body) AS body_hash, structured_metadata\n\
             FROM log_samples\n\
             PREWHERE service IN ('checkout', 'billing')\n\
             WHERE org_id = '' AND fingerprint IN (toUInt128('1'), toUInt128('2'))\n\
             \x20 AND timestamp_ns >= 1500 AND timestamp_ns <= 2000\n\
             \x20 AND (timestamp_ns, fingerprint, cityHash64(body)) >= (1500, toUInt128('7'), 42)\n\
             \x20 AND body LIKE '%err%'\n\
             ORDER BY timestamp_ns ASC, fingerprint ASC, body_hash ASC, body ASC\n\
             LIMIT 500 OFFSET 3"
        );
    }

    /// Issue #90 (backward paging): the query default's newest-first
    /// keyset — DESC on every ORDER column, the exclusive API `start` as
    /// the fixed lower bound, and the mirrored redundant `timestamp_ns <=
    /// end` upper bound so the first page's granule pruning matches the
    /// forward form.
    #[test]
    fn stage3_keyset_backward_first_page_is_byte_exact_desc_with_the_window_bounds() {
        let sql = stage3_keyset(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            "log_samples",
            &[literal("checkout")],
            &[Fingerprint::from_raw(18374).sql_literal()],
            TimeWindow {
                start_ns: 1_000,
                end_ns: 2_000,
            },
            KeysetLower::First,
            Direction::Backward,
            &[],
            500,
        );
        assert_eq!(
            sql,
            "SELECT fingerprint, timestamp_ns, body, cityHash64(body) AS body_hash, structured_metadata\n\
             FROM log_samples\n\
             PREWHERE service = 'checkout'\n\
             WHERE org_id = '' AND fingerprint IN (toUInt128('18374'))\n\
             \x20 AND timestamp_ns > 1000 AND timestamp_ns <= 2000\n\
             ORDER BY timestamp_ns DESC, fingerprint DESC, body_hash DESC, body DESC\n\
             LIMIT 500"
        );
    }

    /// Issue #90 (backward paging): a later newest-first page — the
    /// inclusive `<=` composite tuple, the redundant `timestamp_ns <= ts`
    /// upper bound (walking down), the fixed exclusive `timestamp_ns >
    /// start` lower bound, DESC total order, and the occurrence-count
    /// `OFFSET` — byte-exact.
    #[test]
    fn stage3_keyset_backward_later_page_is_byte_exact_with_le_tuple_and_offset() {
        let sql = stage3_keyset(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            "log_samples",
            &[literal("checkout"), literal("billing")],
            &[
                Fingerprint::from_raw(1).sql_literal(),
                Fingerprint::from_raw(2).sql_literal(),
            ],
            TimeWindow {
                start_ns: 1_000,
                end_ns: 2_000,
            },
            KeysetLower::After {
                tuple: (1_500, Fingerprint::from_raw(7).sql_literal(), 42),
                offset: 3,
            },
            Direction::Backward,
            &[err_line_filter()],
            500,
        );
        assert_eq!(
            sql,
            "SELECT fingerprint, timestamp_ns, body, cityHash64(body) AS body_hash, structured_metadata\n\
             FROM log_samples\n\
             PREWHERE service IN ('checkout', 'billing')\n\
             WHERE org_id = '' AND fingerprint IN (toUInt128('1'), toUInt128('2'))\n\
             \x20 AND timestamp_ns > 1000 AND timestamp_ns <= 1500\n\
             \x20 AND (timestamp_ns, fingerprint, cityHash64(body)) <= (1500, toUInt128('7'), 42)\n\
             \x20 AND body LIKE '%err%'\n\
             ORDER BY timestamp_ns DESC, fingerprint DESC, body_hash DESC, body DESC\n\
             LIMIT 500 OFFSET 3"
        );
    }

    /// Issue #74 pre-query clamp AC (hermetic half): the caller-clamped
    /// fetch limit is exactly what lands in the SQL `LIMIT` — nothing in
    /// the builder can widen it back.
    #[test]
    fn stage3_keyset_limit_is_the_callers_clamped_fetch_limit_verbatim() {
        let sql = stage3_keyset(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            "log_samples",
            &[literal("checkout")],
            &[Fingerprint::from_raw(1).sql_literal()],
            TimeWindow {
                start_ns: 0,
                end_ns: 10,
            },
            KeysetLower::First,
            Direction::Forward,
            &[],
            5_000,
        );
        assert!(sql.ends_with("LIMIT 5000"), "{sql}");
    }

    /// Issue #74 AC6 (hermetic half): the no-filter stats aggregation is
    /// rollup-served — zero body reads — byte-exact.
    #[test]
    fn log_stats_rollup_is_byte_exact() {
        let sql = log_stats_rollup(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            "log_metrics_5s",
            &[
                Fingerprint::from_raw(18374).sql_literal(),
                Fingerprint::from_raw(99120).sql_literal(),
            ],
            TimeWindow {
                start_ns: 1_000,
                end_ns: 2_000,
            },
        );
        assert_eq!(
            sql,
            "SELECT uniqExact(fingerprint) AS streams, uniqExact(toDate(fromUnixTimestamp64Nano(bucket_ns))) AS chunks, sum(count) AS entries, sum(bytes) AS bytes\n\
             FROM log_metrics_5s\n\
             WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120')) AND bucket_ns > 1000 AND bucket_ns <= 2000"
        );
        assert!(!sql.contains("body"), "rollup stats must never read body");
    }

    /// Issue #171 (M7-C3): the `/api/logs/v1/patterns` aggregation is
    /// byte-exact — the pushed-down top-1000 with the `fingerprint IN` PK
    /// prefix, half-open `[start, end)` window, `step_ns` re-bucketing, and the
    /// `groupArray((ts_ns, cnt))` samples array. Never reads `body`.
    #[test]
    fn log_patterns_read_is_byte_exact() {
        let sql = log_patterns_read(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            "log_patterns",
            &[
                Fingerprint::from_raw(18374).sql_literal(),
                Fingerprint::from_raw(99120).sql_literal(),
            ],
            TimeWindow {
                start_ns: 1_000,
                end_ns: 2_000,
            },
            10_000_000_000,
        );
        assert_eq!(
            sql,
            "SELECT pattern, level, total, arrayFlatten(arrayMap((p, c, n) -> arrayConcat(if(p.2 > 0 OR c > 0, [(p.1, p.2)], []), if(c > 0, arrayMap(z -> (toInt64(z), toUInt64(0)), range(p.1 + 10000000000, n, 10000000000)), [])), pts, arrayCumSum(arrayMap(p -> p.3, pts)), arrayPushBack(arrayPopFront(arrayMap(p -> p.1, pts)), 0))) AS samples\n\
             FROM (\n  \
             SELECT pattern, level, sum(cnt) AS total, arraySort(groupArray((ts_ns, cnt, delta))) AS pts\n  \
             FROM (\n    \
             SELECT pattern, level, point.1 AS ts_ns, sum(point.2) AS cnt, sum(point.3) AS delta\n    \
             FROM (\n      \
             SELECT pattern, level, arraySort(groupArray((bucket_ns, toUInt64(count)))) AS rows\n      \
             FROM log_patterns\n      \
             WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120')) AND bucket_ns >= 1000 AND bucket_ns < 2000\n      \
             GROUP BY fingerprint, pattern, level\n    \
             )\n    \
             ARRAY JOIN arrayConcat(arrayMap(r -> (intDiv(r.1, 10000000000) * 10000000000, r.2, toInt64(0)), rows), arrayFlatten(arrayMap((a, b) -> if(b.1 - a.1 < if(10000000000 = 10000000000, 0, 3600000000000) AND intDiv(a.1, 10000000000) * 10000000000 + 10000000000 < intDiv(b.1, 10000000000) * 10000000000, [(intDiv(a.1, 10000000000) * 10000000000 + 10000000000, toUInt64(0), toInt64(1)), (intDiv(b.1, 10000000000) * 10000000000, toUInt64(0), toInt64(-1))], []), arrayPopBack(rows), arrayPopFront(rows)))) AS point\n    \
             GROUP BY pattern, level, ts_ns\n  \
             )\n  \
             GROUP BY pattern, level\n  \
             ORDER BY total DESC, pattern ASC, level ASC\n  \
             LIMIT 1000\n\
             )\n\
             ORDER BY total DESC, pattern ASC, level ASC"
        );
        assert!(!sql.contains("body"), "patterns must never read body");
    }

    /// Issue #74 AC6 (hermetic half): the line-filtered stats fallback
    /// scans `log_samples` with stage-3's exact PREWHERE + skip-index
    /// line-filter pushdown — byte-exact.
    #[test]
    fn log_stats_raw_is_byte_exact_with_line_filter_pushdown() {
        let sql = log_stats_raw(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            "log_samples",
            &[literal("checkout")],
            &[Fingerprint::from_raw(18374).sql_literal()],
            TimeWindow {
                start_ns: 1_000,
                end_ns: 2_000,
            },
            &[err_line_filter()],
        );
        assert_eq!(
            sql,
            "SELECT uniqExact(fingerprint) AS streams, uniqExact(toDate(fromUnixTimestamp64Nano(timestamp_ns))) AS chunks, count() AS entries, sum(length(body)) AS bytes\n\
             FROM log_samples\n\
             PREWHERE service = 'checkout'\n\
             WHERE org_id = '' AND fingerprint IN (toUInt128('18374'))\n\
             \x20 AND timestamp_ns > 1000 AND timestamp_ns <= 2000\n\
             \x20 AND body LIKE '%err%'"
        );
    }

    #[test]
    fn metric_instant_renders_the_same_prewhere_contract() {
        let sql = metric_instant(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            MetricSource::new("log_samples", MetricShape::RawCount),
            &[literal("checkout")],
            &[Fingerprint::from_raw(1).sql_literal()],
            TimeWindow {
                start_ns: 0,
                end_ns: 100,
            },
            ScanLowerBound::Exclusive,
            &[],
            ScanProjection::WithStructuredMetadata,
        );
        assert!(sql.contains("PREWHERE service = 'checkout'\n"));
        assert!(!sql.contains("intDiv"));
    }

    /// Issue #227 review round 11: an UNDERFLOWED scan widening renders the
    /// lower bound INCLUSIVELY (`>= i64::MIN` — vacuous, so a sample stored
    /// at exactly `i64::MIN` survives the predicate, matching the
    /// reference's `(t-range, t]` whose logical bound sits below the
    /// representable domain), across all four metric builders.
    #[test]
    fn an_underflowed_scan_lower_bound_renders_inclusively() {
        let window = TimeWindow {
            start_ns: i64::MIN,
            end_ns: i64::MIN,
        };
        let raw_source = MetricSource::new("log_samples", MetricShape::RawCount);
        let svc = [literal("checkout")];
        let sliding = metric_raw_samples_sliding(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            "log_samples",
            &svc,
            &[Fingerprint::from_raw(1).sql_literal()],
            window,
            ScanLowerBound::Inclusive,
            &[],
            ScanProjection::WithStructuredMetadata,
        );
        assert!(
            sliding.contains(
                "timestamp_ns >= -9223372036854775808 AND timestamp_ns <= -9223372036854775808"
            ),
            "sliding scan must be lower-inclusive on underflow: {sliding}"
        );
        let instant_raw = metric_raw_samples(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            "log_samples",
            &svc,
            &[Fingerprint::from_raw(1).sql_literal()],
            window,
            ScanLowerBound::Inclusive,
            &[],
            ScanProjection::WithStructuredMetadata,
        );
        assert!(
            instant_raw.contains("timestamp_ns >= -9223372036854775808"),
            "instant raw scan must be lower-inclusive on underflow: {instant_raw}"
        );
        let instant_agg = metric_instant(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            raw_source,
            &svc,
            &[Fingerprint::from_raw(1).sql_literal()],
            window,
            ScanLowerBound::Inclusive,
            &[],
            ScanProjection::WithStructuredMetadata,
        );
        assert!(
            instant_agg.contains("timestamp_ns >= -9223372036854775808"),
            "instant aggregate must be lower-inclusive on underflow: {instant_agg}"
        );
        let range_agg = metric_range(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            raw_source,
            &svc,
            &[Fingerprint::from_raw(1).sql_literal()],
            window,
            ScanLowerBound::Inclusive,
            60,
            &[],
        );
        assert!(
            range_agg.contains("timestamp_ns >= -9223372036854775808"),
            "range aggregate must be lower-inclusive on underflow: {range_agg}"
        );
    }

    /// The negative control: an `Exclusive` bound at exactly `i64::MIN` (a
    /// LEGITIMATELY-computed lower bound, e.g. `start = i64::MIN + 1` with
    /// `[1ns]`) keeps the half-open `>` — a sample at exactly `i64::MIN`
    /// stays excluded, as in the reference.
    #[test]
    fn a_legitimate_i64_min_scan_lower_bound_stays_exclusive() {
        let window = TimeWindow {
            start_ns: i64::MIN,
            end_ns: i64::MIN + 1,
        };
        let svc = [literal("checkout")];
        let sliding = metric_raw_samples_sliding(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            "log_samples",
            &svc,
            &[Fingerprint::from_raw(1).sql_literal()],
            window,
            ScanLowerBound::Exclusive,
            &[],
            ScanProjection::WithStructuredMetadata,
        );
        assert!(
            sliding.contains("timestamp_ns > -9223372036854775808 AND"),
            "a non-underflowing i64::MIN bound must stay exclusive: {sliding}"
        );
        assert!(
            !sliding.contains(">="),
            "no inclusive lower bound: {sliding}"
        );
    }

    // -----------------------------------------------------------------
    // W0 (issue #507): the two metric raw-scan builders and the
    // selectivity probe, frozen byte for byte over every argument.
    //
    // Why these three. Measured, one builder at a time: an extra column
    // added to the builder's leading `SELECT` list, then
    // `cargo nextest run -p pulsus-read --no-fail-fast`. Before the three
    // tests below existed, TWO of these builders were reddened by nothing
    // in the crate —
    //
    //     metric_raw_samples           nothing reddened
    //     probe                        nothing reddened
    //     metric_raw_samples_sliding   sql_snapshots::an_unwrapped_sum_over_time_
    //                                  renders_a_sliding_raw_scan_with_no_aggregate_and_no_limit
    //
    // so the sliding builder is here because it shares its four argument
    // axes with `metric_raw_samples`, not because it was uncovered. The
    // gap in the other two is a property of what the existing tests
    // assert, not of the builders: `metric_raw_samples`'s only caller in
    // `tests/sql_snapshots.rs` compares two of its OWN renderings against
    // each other, and a relational equality between two productions of
    // one function cannot see a change to that function.
    //
    // Each test enumerates its builder's arguments rather than sampling
    // them, so a change confined to one argument value cannot pass by
    // landing outside the fixture.
    // -----------------------------------------------------------------

    /// The 36 statements W0 freezes, in the order the three tests below
    /// enumerate them: 16 `metric_raw_samples`, then 16
    /// `metric_raw_samples_sliding`, then 4 `probe`. Each entry is its
    /// axis label and the statement written out line by line.
    #[allow(clippy::type_complexity)]
    const W0_STATEMENTS: [(&str, &[&str]); 36] = [
        (
            "metric_raw_samples Lean Exclusive 1svc nopred",
            &[
                r"SELECT fingerprint, timestamp_ns, body",
                r"FROM log_samples",
                r"PREWHERE service = 'checkout'",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns > 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"ORDER BY timestamp_ns ASC, fingerprint ASC, body ASC",
            ],
        ),
        (
            "metric_raw_samples Lean Exclusive 1svc 1pred",
            &[
                r"SELECT fingerprint, timestamp_ns, body",
                r"FROM log_samples",
                r"PREWHERE service = 'checkout'",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns > 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"  AND body LIKE '%CONN\\_REFUSED%'",
                r"ORDER BY timestamp_ns ASC, fingerprint ASC, body ASC",
            ],
        ),
        (
            "metric_raw_samples Lean Exclusive 3svc nopred",
            &[
                r"SELECT fingerprint, timestamp_ns, body",
                r"FROM log_samples",
                r"PREWHERE service IN ('checkout', 'edge', 'ipcase')",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns > 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"ORDER BY timestamp_ns ASC, fingerprint ASC, body ASC",
            ],
        ),
        (
            "metric_raw_samples Lean Exclusive 3svc 1pred",
            &[
                r"SELECT fingerprint, timestamp_ns, body",
                r"FROM log_samples",
                r"PREWHERE service IN ('checkout', 'edge', 'ipcase')",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns > 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"  AND body LIKE '%CONN\\_REFUSED%'",
                r"ORDER BY timestamp_ns ASC, fingerprint ASC, body ASC",
            ],
        ),
        (
            "metric_raw_samples Lean Inclusive 1svc nopred",
            &[
                r"SELECT fingerprint, timestamp_ns, body",
                r"FROM log_samples",
                r"PREWHERE service = 'checkout'",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns >= 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"ORDER BY timestamp_ns ASC, fingerprint ASC, body ASC",
            ],
        ),
        (
            "metric_raw_samples Lean Inclusive 1svc 1pred",
            &[
                r"SELECT fingerprint, timestamp_ns, body",
                r"FROM log_samples",
                r"PREWHERE service = 'checkout'",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns >= 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"  AND body LIKE '%CONN\\_REFUSED%'",
                r"ORDER BY timestamp_ns ASC, fingerprint ASC, body ASC",
            ],
        ),
        (
            "metric_raw_samples Lean Inclusive 3svc nopred",
            &[
                r"SELECT fingerprint, timestamp_ns, body",
                r"FROM log_samples",
                r"PREWHERE service IN ('checkout', 'edge', 'ipcase')",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns >= 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"ORDER BY timestamp_ns ASC, fingerprint ASC, body ASC",
            ],
        ),
        (
            "metric_raw_samples Lean Inclusive 3svc 1pred",
            &[
                r"SELECT fingerprint, timestamp_ns, body",
                r"FROM log_samples",
                r"PREWHERE service IN ('checkout', 'edge', 'ipcase')",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns >= 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"  AND body LIKE '%CONN\\_REFUSED%'",
                r"ORDER BY timestamp_ns ASC, fingerprint ASC, body ASC",
            ],
        ),
        (
            "metric_raw_samples WithStructuredMetadata Exclusive 1svc nopred",
            &[
                r"SELECT fingerprint, timestamp_ns, body, structured_metadata",
                r"FROM log_samples",
                r"PREWHERE service = 'checkout'",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns > 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"ORDER BY timestamp_ns ASC, fingerprint ASC, body ASC",
            ],
        ),
        (
            "metric_raw_samples WithStructuredMetadata Exclusive 1svc 1pred",
            &[
                r"SELECT fingerprint, timestamp_ns, body, structured_metadata",
                r"FROM log_samples",
                r"PREWHERE service = 'checkout'",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns > 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"  AND body LIKE '%CONN\\_REFUSED%'",
                r"ORDER BY timestamp_ns ASC, fingerprint ASC, body ASC",
            ],
        ),
        (
            "metric_raw_samples WithStructuredMetadata Exclusive 3svc nopred",
            &[
                r"SELECT fingerprint, timestamp_ns, body, structured_metadata",
                r"FROM log_samples",
                r"PREWHERE service IN ('checkout', 'edge', 'ipcase')",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns > 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"ORDER BY timestamp_ns ASC, fingerprint ASC, body ASC",
            ],
        ),
        (
            "metric_raw_samples WithStructuredMetadata Exclusive 3svc 1pred",
            &[
                r"SELECT fingerprint, timestamp_ns, body, structured_metadata",
                r"FROM log_samples",
                r"PREWHERE service IN ('checkout', 'edge', 'ipcase')",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns > 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"  AND body LIKE '%CONN\\_REFUSED%'",
                r"ORDER BY timestamp_ns ASC, fingerprint ASC, body ASC",
            ],
        ),
        (
            "metric_raw_samples WithStructuredMetadata Inclusive 1svc nopred",
            &[
                r"SELECT fingerprint, timestamp_ns, body, structured_metadata",
                r"FROM log_samples",
                r"PREWHERE service = 'checkout'",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns >= 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"ORDER BY timestamp_ns ASC, fingerprint ASC, body ASC",
            ],
        ),
        (
            "metric_raw_samples WithStructuredMetadata Inclusive 1svc 1pred",
            &[
                r"SELECT fingerprint, timestamp_ns, body, structured_metadata",
                r"FROM log_samples",
                r"PREWHERE service = 'checkout'",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns >= 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"  AND body LIKE '%CONN\\_REFUSED%'",
                r"ORDER BY timestamp_ns ASC, fingerprint ASC, body ASC",
            ],
        ),
        (
            "metric_raw_samples WithStructuredMetadata Inclusive 3svc nopred",
            &[
                r"SELECT fingerprint, timestamp_ns, body, structured_metadata",
                r"FROM log_samples",
                r"PREWHERE service IN ('checkout', 'edge', 'ipcase')",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns >= 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"ORDER BY timestamp_ns ASC, fingerprint ASC, body ASC",
            ],
        ),
        (
            "metric_raw_samples WithStructuredMetadata Inclusive 3svc 1pred",
            &[
                r"SELECT fingerprint, timestamp_ns, body, structured_metadata",
                r"FROM log_samples",
                r"PREWHERE service IN ('checkout', 'edge', 'ipcase')",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns >= 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"  AND body LIKE '%CONN\\_REFUSED%'",
                r"ORDER BY timestamp_ns ASC, fingerprint ASC, body ASC",
            ],
        ),
        (
            "metric_raw_samples_sliding Lean Exclusive 1svc nopred",
            &[
                r"SELECT fingerprint, timestamp_ns, body",
                r"FROM log_samples",
                r"PREWHERE service = 'checkout'",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns > 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"ORDER BY service ASC, fingerprint ASC, timestamp_ns ASC",
            ],
        ),
        (
            "metric_raw_samples_sliding Lean Exclusive 1svc 1pred",
            &[
                r"SELECT fingerprint, timestamp_ns, body",
                r"FROM log_samples",
                r"PREWHERE service = 'checkout'",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns > 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"  AND body LIKE '%CONN\\_REFUSED%'",
                r"ORDER BY service ASC, fingerprint ASC, timestamp_ns ASC",
            ],
        ),
        (
            "metric_raw_samples_sliding Lean Exclusive 3svc nopred",
            &[
                r"SELECT fingerprint, timestamp_ns, body",
                r"FROM log_samples",
                r"PREWHERE service IN ('checkout', 'edge', 'ipcase')",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns > 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"ORDER BY service ASC, fingerprint ASC, timestamp_ns ASC",
            ],
        ),
        (
            "metric_raw_samples_sliding Lean Exclusive 3svc 1pred",
            &[
                r"SELECT fingerprint, timestamp_ns, body",
                r"FROM log_samples",
                r"PREWHERE service IN ('checkout', 'edge', 'ipcase')",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns > 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"  AND body LIKE '%CONN\\_REFUSED%'",
                r"ORDER BY service ASC, fingerprint ASC, timestamp_ns ASC",
            ],
        ),
        (
            "metric_raw_samples_sliding Lean Inclusive 1svc nopred",
            &[
                r"SELECT fingerprint, timestamp_ns, body",
                r"FROM log_samples",
                r"PREWHERE service = 'checkout'",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns >= 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"ORDER BY service ASC, fingerprint ASC, timestamp_ns ASC",
            ],
        ),
        (
            "metric_raw_samples_sliding Lean Inclusive 1svc 1pred",
            &[
                r"SELECT fingerprint, timestamp_ns, body",
                r"FROM log_samples",
                r"PREWHERE service = 'checkout'",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns >= 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"  AND body LIKE '%CONN\\_REFUSED%'",
                r"ORDER BY service ASC, fingerprint ASC, timestamp_ns ASC",
            ],
        ),
        (
            "metric_raw_samples_sliding Lean Inclusive 3svc nopred",
            &[
                r"SELECT fingerprint, timestamp_ns, body",
                r"FROM log_samples",
                r"PREWHERE service IN ('checkout', 'edge', 'ipcase')",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns >= 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"ORDER BY service ASC, fingerprint ASC, timestamp_ns ASC",
            ],
        ),
        (
            "metric_raw_samples_sliding Lean Inclusive 3svc 1pred",
            &[
                r"SELECT fingerprint, timestamp_ns, body",
                r"FROM log_samples",
                r"PREWHERE service IN ('checkout', 'edge', 'ipcase')",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns >= 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"  AND body LIKE '%CONN\\_REFUSED%'",
                r"ORDER BY service ASC, fingerprint ASC, timestamp_ns ASC",
            ],
        ),
        (
            "metric_raw_samples_sliding WithStructuredMetadata Exclusive 1svc nopred",
            &[
                r"SELECT fingerprint, timestamp_ns, body, structured_metadata",
                r"FROM log_samples",
                r"PREWHERE service = 'checkout'",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns > 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"ORDER BY service ASC, fingerprint ASC, timestamp_ns ASC",
            ],
        ),
        (
            "metric_raw_samples_sliding WithStructuredMetadata Exclusive 1svc 1pred",
            &[
                r"SELECT fingerprint, timestamp_ns, body, structured_metadata",
                r"FROM log_samples",
                r"PREWHERE service = 'checkout'",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns > 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"  AND body LIKE '%CONN\\_REFUSED%'",
                r"ORDER BY service ASC, fingerprint ASC, timestamp_ns ASC",
            ],
        ),
        (
            "metric_raw_samples_sliding WithStructuredMetadata Exclusive 3svc nopred",
            &[
                r"SELECT fingerprint, timestamp_ns, body, structured_metadata",
                r"FROM log_samples",
                r"PREWHERE service IN ('checkout', 'edge', 'ipcase')",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns > 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"ORDER BY service ASC, fingerprint ASC, timestamp_ns ASC",
            ],
        ),
        (
            "metric_raw_samples_sliding WithStructuredMetadata Exclusive 3svc 1pred",
            &[
                r"SELECT fingerprint, timestamp_ns, body, structured_metadata",
                r"FROM log_samples",
                r"PREWHERE service IN ('checkout', 'edge', 'ipcase')",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns > 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"  AND body LIKE '%CONN\\_REFUSED%'",
                r"ORDER BY service ASC, fingerprint ASC, timestamp_ns ASC",
            ],
        ),
        (
            "metric_raw_samples_sliding WithStructuredMetadata Inclusive 1svc nopred",
            &[
                r"SELECT fingerprint, timestamp_ns, body, structured_metadata",
                r"FROM log_samples",
                r"PREWHERE service = 'checkout'",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns >= 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"ORDER BY service ASC, fingerprint ASC, timestamp_ns ASC",
            ],
        ),
        (
            "metric_raw_samples_sliding WithStructuredMetadata Inclusive 1svc 1pred",
            &[
                r"SELECT fingerprint, timestamp_ns, body, structured_metadata",
                r"FROM log_samples",
                r"PREWHERE service = 'checkout'",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns >= 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"  AND body LIKE '%CONN\\_REFUSED%'",
                r"ORDER BY service ASC, fingerprint ASC, timestamp_ns ASC",
            ],
        ),
        (
            "metric_raw_samples_sliding WithStructuredMetadata Inclusive 3svc nopred",
            &[
                r"SELECT fingerprint, timestamp_ns, body, structured_metadata",
                r"FROM log_samples",
                r"PREWHERE service IN ('checkout', 'edge', 'ipcase')",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns >= 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"ORDER BY service ASC, fingerprint ASC, timestamp_ns ASC",
            ],
        ),
        (
            "metric_raw_samples_sliding WithStructuredMetadata Inclusive 3svc 1pred",
            &[
                r"SELECT fingerprint, timestamp_ns, body, structured_metadata",
                r"FROM log_samples",
                r"PREWHERE service IN ('checkout', 'edge', 'ipcase')",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns >= 1782906900000000000 AND timestamp_ns <= 1782928800000000000",
                r"  AND body LIKE '%CONN\\_REFUSED%'",
                r"ORDER BY service ASC, fingerprint ASC, timestamp_ns ASC",
            ],
        ),
        (
            "probe 1mo plain",
            &[
                r"SELECT count() AS n",
                r"FROM log_streams_idx",
                r"WHERE org_id = '' AND month = '2026-07-01' AND key = 'service_name'",
            ],
        ),
        (
            "probe 1mo escaped",
            &[
                r"SELECT count() AS n",
                r"FROM log_streams_idx",
                r"WHERE org_id = '' AND month = '2026-07-01' AND key = 'a\'b\\c'",
            ],
        ),
        (
            "probe 2mo plain",
            &[
                r"SELECT count() AS n",
                r"FROM log_streams_idx",
                r"WHERE org_id = '' AND month IN ('2026-07-01', '2026-08-01') AND key = 'service_name'",
            ],
        ),
        (
            "probe 2mo escaped",
            &[
                r"SELECT count() AS n",
                r"FROM log_streams_idx",
                r"WHERE org_id = '' AND month IN ('2026-07-01', '2026-08-01') AND key = 'a\'b\\c'",
            ],
        ),
    ];

    /// The emit grid and scan bounds every bucketed statement below is
    /// rendered from: a one-minute step on a grid whose first point is
    /// `1700000000000000000`, so `lo_ns` is one step below it, and a scan
    /// start that equals `lo_ns` because this fixture's range equals its
    /// step.
    const BUCKETED_SCAN: BucketedScan = BucketedScan {
        window: TimeWindow {
            start_ns: 1_699_999_940_000_000_000,
            end_ns: 1_700_003_600_000_000_000,
        },
        lower: ScanLowerBound::Exclusive,
        lo_ns: 1_699_999_940_000_000_000,
        step_ns: 60_000_000_000,
    };

    /// One statement of [`W0_STATEMENTS`], joined back into the text the
    /// builder produces.
    fn w0_expected(index: usize) -> String {
        W0_STATEMENTS[index].1.join("\n")
    }

    /// Every argument of the three frozen builders, given a value. These
    /// are held FIXED across all 36 rows, so a difference between two
    /// rendered statements is a difference the builder made and not one
    /// the fixture made.
    ///
    /// **Not enumerated, named here so the tests are not read as
    /// exhaustive:** the fingerprint count and values, the table names,
    /// the concrete window bounds, a second or third extra predicate,
    /// zero or more than two months, and broader key spellings.
    struct W0Fixtures {
        fingerprints: Vec<FpLiteral>,
        window: TimeWindow,
        no_service: Vec<CheckedLiteral>,
        one_service: Vec<CheckedLiteral>,
        three_services: Vec<CheckedLiteral>,
        no_predicate: Vec<CheckedFragment>,
        one_predicate: Vec<CheckedFragment>,
        one_month: Vec<MonthLiteral>,
        two_months: Vec<MonthLiteral>,
        plain_key: CheckedLiteral,
        escaped_key: CheckedLiteral,
    }

    impl W0Fixtures {
        fn new() -> Self {
            let one_predicate = vec![
                crate::logql::predicate::line_filter(&pulsus_logql::LineFilter {
                    op: pulsus_logql::LineFilterOp::Contains,
                    value: "CONN_REFUSED".to_string(),
                    value_is_ip: false,
                    or_matches: Vec::new(),
                })
                .expect("a Contains filter compiles no regex"),
            ];
            W0Fixtures {
                fingerprints: [18374, 99120]
                    .map(|v| Fingerprint::from_raw(v).sql_literal())
                    .to_vec(),
                window: TimeWindow {
                    start_ns: 1_782_906_900_000_000_000,
                    end_ns: 1_782_928_800_000_000_000,
                },
                no_service: Vec::new(),
                one_service: vec![literal("checkout")],
                three_services: vec![literal("checkout"), literal("edge"), literal("ipcase")],
                no_predicate: Vec::new(),
                one_predicate,
                one_month: vec![month_literal(2026, 7)],
                two_months: vec![month_literal(2026, 7), month_literal(2026, 8)],
                plain_key: literal("service_name"),
                escaped_key: literal("a'b\\c"),
            }
        }

        /// The scan builders' four axes, in the order [`W0_STATEMENTS`]
        /// lists them: projection, then lower bound, then service count,
        /// then predicate presence.
        fn scan_axes(
            &self,
        ) -> Vec<(
            ScanProjection,
            ScanLowerBound,
            &[CheckedLiteral],
            &[CheckedFragment],
        )> {
            let mut rows = Vec::new();
            for projection in [ScanProjection::Lean, ScanProjection::WithStructuredMetadata] {
                for lower in [ScanLowerBound::Exclusive, ScanLowerBound::Inclusive] {
                    for services in [&self.one_service, &self.three_services] {
                        for predicates in [&self.no_predicate, &self.one_predicate] {
                            rows.push((
                                projection,
                                lower,
                                services.as_slice(),
                                predicates.as_slice(),
                            ));
                        }
                    }
                }
            }
            rows
        }
    }

    /// Asserts that each of `rendered` equals its [`W0_STATEMENTS`] entry
    /// and that no entry outside this block renders the same text — so
    /// the 36 statements are distinct as a whole rather than only within
    /// one builder's block.
    fn assert_w0_block(rendered: &[String], first_index: usize) {
        for (offset, sql) in rendered.iter().enumerate() {
            let index = first_index + offset;
            let (label, _) = W0_STATEMENTS[index];
            assert_eq!(
                *sql,
                w0_expected(index),
                "W0 statement {} ({label}) is not the frozen text",
                index + 1
            );
            for (other, (other_label, other_lines)) in W0_STATEMENTS.iter().enumerate() {
                if other == index {
                    continue;
                }
                assert_ne!(
                    other_lines.join("\n"),
                    *sql,
                    "W0 statements {} ({label}) and {} ({other_label}) render the same text, so \
                     one of them proves nothing",
                    index + 1,
                    other + 1,
                );
            }
        }
    }

    /// W0, criterion 1 (issue #507): 16 rows — `ScanProjection` ×
    /// `ScanLowerBound` × {one service, three services} × {no extra
    /// predicate, one}.
    #[test]
    fn metric_raw_samples_is_byte_exact_over_projection_lower_bound_service_count_and_predicate_presence()
     {
        let f = W0Fixtures::new();
        let rendered: Vec<String> = f
            .scan_axes()
            .into_iter()
            .map(|(projection, lower, services, predicates)| {
                metric_raw_samples(
                    &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                    "log_samples",
                    services,
                    &f.fingerprints,
                    f.window,
                    lower,
                    predicates,
                    projection,
                )
            })
            .collect();
        assert_eq!(rendered.len(), 16);
        assert_w0_block(&rendered, 0);
    }

    /// W0, criterion 1 (issue #507): the same 16 rows through the sliding
    /// builder, whose `ORDER BY` is the physical primary key.
    #[test]
    fn metric_raw_samples_sliding_is_byte_exact_over_the_same_four_axes() {
        let f = W0Fixtures::new();
        let rendered: Vec<String> = f
            .scan_axes()
            .into_iter()
            .map(|(projection, lower, services, predicates)| {
                metric_raw_samples_sliding(
                    &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                    "log_samples",
                    services,
                    &f.fingerprints,
                    f.window,
                    lower,
                    predicates,
                    projection,
                )
            })
            .collect();
        assert_eq!(rendered.len(), 16);
        assert_w0_block(&rendered, 16);
    }

    /// W0, criterion 1 (issue #507): 4 rows — {one month, two months} ×
    /// {a plain key, a key needing escaping}.
    #[test]
    fn probe_is_byte_exact_over_month_count_and_key_escaping() {
        let f = W0Fixtures::new();
        let mut rendered = Vec::new();
        for months in [&f.one_month, &f.two_months] {
            for key in [&f.plain_key, &f.escaped_key] {
                rendered.push(probe(
                    &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                    "log_streams_idx",
                    months,
                    key,
                ));
            }
        }
        assert_eq!(rendered.len(), 4);
        assert_w0_block(&rendered, 32);
    }

    // -----------------------------------------------------------------
    // W2 (issue #507): the bucketed range read, frozen byte for byte, and
    // the three ways its grid can refuse.
    //
    // The statement in row 1 is the one the plan prints. Nothing else in
    // the crate renders this text, so these assertions pin a decision
    // rather than confirming one — the decision being that the grid is an
    // anchored ceiling and not the epoch-anchored floor the shipped range
    // renderer emits. What confirms it is
    // `the_anchored_bucket_expression_is_the_grid_the_window_implies` on
    // the live leg, which executes the expression.
    // -----------------------------------------------------------------

    /// The eleven statements W2's bucketed builder freezes, in the order
    /// the test below enumerates them.
    #[allow(clippy::type_complexity)]
    const BUCKETED_STATEMENTS: [(&str, &[&str]); 11] = [
        (
            "Lean RawCount Exclusive 1svc nopred",
            &[
                r"SELECT fingerprint, 1699999940000000000 + intDiv(timestamp_ns - 1699999940000000000 + 60000000000 - 1, 60000000000) * 60000000000 AS bucket_ns, count() AS n",
                r"FROM log_samples",
                r"PREWHERE service = 'checkout'",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns > 1699999940000000000 AND timestamp_ns <= 1700003600000000000",
                r"GROUP BY fingerprint, bucket_ns",
            ],
        ),
        (
            "Lean RawCount Inclusive 1svc nopred",
            &[
                r"SELECT fingerprint, 1699999940000000000 + intDiv(timestamp_ns - 1699999940000000000 + 60000000000 - 1, 60000000000) * 60000000000 AS bucket_ns, count() AS n",
                r"FROM log_samples",
                r"PREWHERE service = 'checkout'",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns >= 1699999940000000000 AND timestamp_ns <= 1700003600000000000",
                r"GROUP BY fingerprint, bucket_ns",
            ],
        ),
        (
            "Lean RawBytes Exclusive 1svc nopred",
            &[
                r"SELECT fingerprint, 1699999940000000000 + intDiv(timestamp_ns - 1699999940000000000 + 60000000000 - 1, 60000000000) * 60000000000 AS bucket_ns, sum(length(body)) AS n",
                r"FROM log_samples",
                r"PREWHERE service = 'checkout'",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns > 1699999940000000000 AND timestamp_ns <= 1700003600000000000",
                r"GROUP BY fingerprint, bucket_ns",
            ],
        ),
        (
            "Lean RawBytes Inclusive 1svc nopred",
            &[
                r"SELECT fingerprint, 1699999940000000000 + intDiv(timestamp_ns - 1699999940000000000 + 60000000000 - 1, 60000000000) * 60000000000 AS bucket_ns, sum(length(body)) AS n",
                r"FROM log_samples",
                r"PREWHERE service = 'checkout'",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns >= 1699999940000000000 AND timestamp_ns <= 1700003600000000000",
                r"GROUP BY fingerprint, bucket_ns",
            ],
        ),
        (
            "WithStructuredMetadata RawCount Exclusive 1svc nopred",
            &[
                r"SELECT fingerprint, 1699999940000000000 + intDiv(timestamp_ns - 1699999940000000000 + 60000000000 - 1, 60000000000) * 60000000000 AS bucket_ns, count() AS n, structured_metadata",
                r"FROM log_samples",
                r"PREWHERE service = 'checkout'",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns > 1699999940000000000 AND timestamp_ns <= 1700003600000000000",
                r"GROUP BY fingerprint, bucket_ns, structured_metadata",
            ],
        ),
        (
            "WithStructuredMetadata RawCount Inclusive 1svc nopred",
            &[
                r"SELECT fingerprint, 1699999940000000000 + intDiv(timestamp_ns - 1699999940000000000 + 60000000000 - 1, 60000000000) * 60000000000 AS bucket_ns, count() AS n, structured_metadata",
                r"FROM log_samples",
                r"PREWHERE service = 'checkout'",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns >= 1699999940000000000 AND timestamp_ns <= 1700003600000000000",
                r"GROUP BY fingerprint, bucket_ns, structured_metadata",
            ],
        ),
        (
            "WithStructuredMetadata RawBytes Exclusive 1svc nopred",
            &[
                r"SELECT fingerprint, 1699999940000000000 + intDiv(timestamp_ns - 1699999940000000000 + 60000000000 - 1, 60000000000) * 60000000000 AS bucket_ns, sum(length(body)) AS n, structured_metadata",
                r"FROM log_samples",
                r"PREWHERE service = 'checkout'",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns > 1699999940000000000 AND timestamp_ns <= 1700003600000000000",
                r"GROUP BY fingerprint, bucket_ns, structured_metadata",
            ],
        ),
        (
            "WithStructuredMetadata RawBytes Inclusive 1svc nopred",
            &[
                r"SELECT fingerprint, 1699999940000000000 + intDiv(timestamp_ns - 1699999940000000000 + 60000000000 - 1, 60000000000) * 60000000000 AS bucket_ns, sum(length(body)) AS n, structured_metadata",
                r"FROM log_samples",
                r"PREWHERE service = 'checkout'",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns >= 1699999940000000000 AND timestamp_ns <= 1700003600000000000",
                r"GROUP BY fingerprint, bucket_ns, structured_metadata",
            ],
        ),
        (
            "WithStructuredMetadata RawCount Exclusive 3svc nopred",
            &[
                r"SELECT fingerprint, 1699999940000000000 + intDiv(timestamp_ns - 1699999940000000000 + 60000000000 - 1, 60000000000) * 60000000000 AS bucket_ns, count() AS n, structured_metadata",
                r"FROM log_samples",
                r"PREWHERE service IN ('checkout', 'edge', 'ipcase')",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns > 1699999940000000000 AND timestamp_ns <= 1700003600000000000",
                r"GROUP BY fingerprint, bucket_ns, structured_metadata",
            ],
        ),
        (
            "WithStructuredMetadata RawCount Exclusive 1svc 1pred",
            &[
                r"SELECT fingerprint, 1699999940000000000 + intDiv(timestamp_ns - 1699999940000000000 + 60000000000 - 1, 60000000000) * 60000000000 AS bucket_ns, count() AS n, structured_metadata",
                r"FROM log_samples",
                r"PREWHERE service = 'checkout'",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns > 1699999940000000000 AND timestamp_ns <= 1700003600000000000",
                r"  AND body LIKE '%CONN\\_REFUSED%'",
                r"GROUP BY fingerprint, bucket_ns, structured_metadata",
            ],
        ),
        (
            "WithStructuredMetadata RawCount Exclusive 0svc nopred",
            &[
                r"SELECT fingerprint, 1699999940000000000 + intDiv(timestamp_ns - 1699999940000000000 + 60000000000 - 1, 60000000000) * 60000000000 AS bucket_ns, count() AS n, structured_metadata",
                r"FROM log_samples",
                r"WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
                r"  AND timestamp_ns > 1699999940000000000 AND timestamp_ns <= 1700003600000000000",
                r"GROUP BY fingerprint, bucket_ns, structured_metadata",
            ],
        ),
    ];

    /// W2 (issue #507): the bucketed builder is byte-exact over its
    /// projection, its reducer shape, its scan lower bound, its service
    /// count — including the empty list, which renders no `PREWHERE` line
    /// at all — and the presence of an extra predicate.
    ///
    /// **Not enumerated**, so the test is not read as exhaustive: the
    /// fingerprint list, the table name, the window, the grid, more than
    /// one extra predicate, and the two rollup shapes, which cannot reach
    /// this builder because the rollup table has no `structured_metadata`
    /// column.
    #[test]
    fn metric_range_bucketed_is_byte_exact_over_projection_shape_bound_services_and_predicate() {
        let f = W0Fixtures::new();
        let mut rendered: Vec<String> = Vec::new();
        for projection in [ScanProjection::Lean, ScanProjection::WithStructuredMetadata] {
            for shape in [MetricShape::RawCount, MetricShape::RawBytes] {
                for lower in [ScanLowerBound::Exclusive, ScanLowerBound::Inclusive] {
                    let mut scan = BUCKETED_SCAN;
                    scan.lower = lower;
                    rendered.push(
                        metric_range_bucketed(
                            &pulsus_model::Tenant::from_header(None, false)
                                .expect("the empty tenant"),
                            MetricSource::new("log_samples", shape),
                            &f.one_service,
                            &f.fingerprints,
                            scan,
                            &f.no_predicate,
                            projection,
                        )
                        .expect("a renderable grid"),
                    );
                }
            }
        }
        for (services, predicates) in [
            (&f.three_services, &f.no_predicate),
            (&f.one_service, &f.one_predicate),
            (&f.no_service, &f.no_predicate),
        ] {
            rendered.push(
                metric_range_bucketed(
                    &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                    MetricSource::new("log_samples", MetricShape::RawCount),
                    services,
                    &f.fingerprints,
                    BUCKETED_SCAN,
                    predicates,
                    ScanProjection::WithStructuredMetadata,
                )
                .expect("a renderable grid"),
            );
        }
        assert_eq!(rendered.len(), BUCKETED_STATEMENTS.len());
        for (i, sql) in rendered.iter().enumerate() {
            let (label, lines) = BUCKETED_STATEMENTS[i];
            assert_eq!(
                *sql,
                lines.join("\n"),
                "bucketed statement {} ({label}) is not the frozen text",
                i + 1
            );
        }
        // The eleven differ from one another, so no row proves nothing by
        // being a duplicate of its neighbour.
        let mut seen: Vec<&String> = rendered.iter().collect();
        seen.sort();
        seen.dedup();
        assert_eq!(seen.len(), rendered.len(), "two rows render the same text");
    }

    /// W2 (issue #507): the grid anchor is the emit grid's, never the
    /// scan's own start.
    ///
    /// The two coincide exactly when the range equals the step, which is
    /// the case that would make a wrong implementation look right. Here
    /// the range is half the step, so the widened scan start sits half a
    /// step above `lo_ns` and the two anchors render different text.
    #[test]
    fn the_bucket_anchor_is_the_emit_grid_not_the_widened_scan_start() {
        let f = W0Fixtures::new();
        let grid_start_ns = 1_700_000_000_000_000_000i64;
        let step_ns = 60_000_000_000i64;
        let range_ns = 30_000_000_000i64;
        let lo_ns = grid_start_ns - step_ns;
        let scan_start_ns = grid_start_ns - range_ns;
        assert_ne!(lo_ns, scan_start_ns, "the fixture must separate the two");

        let render = |lo: i64| {
            metric_range_bucketed(
                &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                MetricSource::new("log_samples", MetricShape::RawCount),
                &f.one_service,
                &f.fingerprints,
                BucketedScan {
                    window: TimeWindow {
                        start_ns: scan_start_ns,
                        end_ns: 1_700_003_600_000_000_000,
                    },
                    lower: ScanLowerBound::Exclusive,
                    lo_ns: lo,
                    step_ns,
                },
                &f.no_predicate,
                ScanProjection::WithStructuredMetadata,
            )
            .expect("a renderable grid")
        };
        assert!(
            render(lo_ns).contains(
                "1699999940000000000 + intDiv(timestamp_ns - 1699999940000000000 + 60000000000 \
                 - 1, 60000000000) * 60000000000"
            ),
            "the emit grid's anchor is `grid_start_ns - step_ns`"
        );
        assert_ne!(
            render(lo_ns),
            render(scan_start_ns),
            "anchoring on the scan start must render different text, or this test cannot see \
             the substitution it exists to catch"
        );
    }

    /// N1's statement (issue #624, §7.4 of its plan): count, one service, no
    /// predicate, `[150s]` at a 60 s step over one hour.
    const SLIDING_COUNT: &[&str] = &[
        "SELECT fingerprint, toInt64(1700000000000000000 + k * 60000000000) AS bucket_ns, toUInt64(v) AS n, structured_metadata",
        "FROM (",
        "  SELECT fingerprint, structured_metadata, k0,",
        "         sum(dv) OVER cum AS v, sum(dc) OVER cum AS p, leadInFrame(k0, 1, 61) OVER whole AS k1",
        "  FROM (",
        "    SELECT fingerprint, structured_metadata, d.1 AS k0, sum(d.2) AS dv, sum(d.3) AS dc",
        "    FROM (",
        "      SELECT fingerprint, structured_metadata,",
        "             greatest(intDiv(timestamp_ns - 1699999820000000000 + 59999999999, 60000000000) - 3, 0) AS lo,",
        "             least(intDiv(timestamp_ns - 1699999670000000000 + 59999999999, 60000000000) - 4, 60) AS hi,",
        "             count() AS m, count() AS c",
        "      FROM log_samples",
        "      PREWHERE service = 'checkout'",
        "      WHERE org_id = '' AND fingerprint IN (toUInt128('18374'), toUInt128('99120'))",
        "        AND timestamp_ns > 1699999850000000000 AND timestamp_ns <= 1700003600000000000",
        "      GROUP BY fingerprint, structured_metadata, lo, hi",
        "      HAVING lo <= hi",
        "    )",
        "    ARRAY JOIN [(lo, toInt64(m), toInt64(c)), (hi + 1, -toInt64(m), -toInt64(c))] AS d",
        "    GROUP BY fingerprint, structured_metadata, k0",
        "  )",
        "  WINDOW cum AS (PARTITION BY fingerprint, structured_metadata ORDER BY k0 ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),",
        "         whole AS (PARTITION BY fingerprint, structured_metadata ORDER BY k0 ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
        ")",
        "ARRAY JOIN range(k0, least(k1, 61)) AS k",
        "WHERE p > 0",
    ];

    const SLIDING_SCAN: SlidingScan = SlidingScan {
        window: TimeWindow {
            start_ns: 1_699_999_850_000_000_000,
            end_ns: 1_700_003_600_000_000_000,
        },
        lower: ScanLowerBound::Exclusive,
        grid_start_ns: 1_700_000_000_000_000_000,
        step_ns: 60_000_000_000,
        range_ns: 150_000_000_000,
    };

    /// N1 (issue #624): the sliding statement is byte-exact. The count form
    /// is the frozen text; the bytes form differs only in its `m`; three
    /// services and one pushed line filter render as the bucketed statement
    /// renders them. Four statements, all distinct.
    #[test]
    fn metric_range_sliding_is_byte_exact() {
        let f = W0Fixtures::new();
        let render =
            |shape: MetricShape, services: &[CheckedLiteral], predicates: &[CheckedFragment]| {
                metric_range_sliding(
                    &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                    MetricSource::new("log_samples", shape),
                    services,
                    &f.fingerprints,
                    SLIDING_SCAN,
                    predicates,
                )
                .expect("a renderable sliding statement")
            };
        let count = SLIDING_COUNT.join("\n");
        let bytes = count.replace(
            "             count() AS m, count() AS c",
            "             sum(length(body)) AS m, count() AS c",
        );
        let three = count.replace(
            "PREWHERE service = 'checkout'",
            "PREWHERE service IN ('checkout', 'edge', 'ipcase')",
        );
        let with_predicate = count.replace(
            "AND timestamp_ns <= 1700003600000000000\n",
            "AND timestamp_ns <= 1700003600000000000\n        AND body LIKE '%CONN\\\\_REFUSED%'\n",
        );
        let cases = [
            (
                "count",
                render(MetricShape::RawCount, &f.one_service, &f.no_predicate),
                count,
            ),
            (
                "bytes",
                render(MetricShape::RawBytes, &f.one_service, &f.no_predicate),
                bytes,
            ),
            (
                "three services",
                render(MetricShape::RawCount, &f.three_services, &f.no_predicate),
                three,
            ),
            (
                "one predicate",
                render(MetricShape::RawCount, &f.one_service, &f.one_predicate),
                with_predicate,
            ),
        ];
        for (label, got, want) in &cases {
            assert_eq!(
                got, want,
                "sliding statement ({label}) is not the frozen text"
            );
        }
        let mut seen: Vec<&String> = cases.iter().map(|(_, got, _)| got).collect();
        seen.sort();
        seen.dedup();
        assert_eq!(seen.len(), cases.len(), "two rows render the same text");
    }

    /// W2 (issue #507): the three ways an anchored grid refuses, each with
    /// its own input.
    ///
    /// None of them is a client error — the query is valid and the link
    /// stays residual, so the evaluator answers as it does today.
    #[test]
    fn an_unrenderable_bucket_grid_refuses_rather_than_wrapping() {
        use crate::logql::predicate::BucketGridRefusal;
        let f = W0Fixtures::new();
        let render = |lo_ns: i64, step_ns: i64, start_ns: i64, end_ns: i64| {
            metric_range_bucketed(
                &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                MetricSource::new("log_samples", MetricShape::RawCount),
                &f.one_service,
                &f.fingerprints,
                BucketedScan {
                    window: TimeWindow { start_ns, end_ns },
                    lower: ScanLowerBound::Exclusive,
                    lo_ns,
                    step_ns,
                },
                &f.no_predicate,
                ScanProjection::WithStructuredMetadata,
            )
        };
        // A zero or negative step has no grid points.
        for step in [0i64, -1] {
            assert_eq!(
                render(0, step, 0, 1_000),
                Err(BucketGridRefusal::StepNotPositive),
                "step {step}"
            );
        }
        // An anchor above the scan start: `intDiv` truncates toward zero,
        // which is a floor only for a non-negative numerator, so a row
        // below the anchor would bucket upward.
        assert_eq!(
            render(1_000, 60, 999, 10_000),
            Err(BucketGridRefusal::AnchorAboveScanStart)
        );
        // The plan's own overflow input.
        assert_eq!(
            render(i64::MAX - 1, 60_000_000_000, 0, 1_700_000_000_000_000_000),
            Err(BucketGridRefusal::AnchorAboveScanStart),
            "an anchor at the top of the axis is refused before any arithmetic is attempted"
        );
        // Genuine overflow: the widest numerator the statement can
        // evaluate is not representable.
        assert_eq!(
            render(i64::MIN, 60_000_000_000, i64::MIN, i64::MAX),
            Err(BucketGridRefusal::WouldOverflow)
        );
        // And the control: an ordinary grid renders.
        assert!(
            render(
                1_699_999_940_000_000_000,
                60_000_000_000,
                1_699_999_940_000_000_000,
                1_700_003_600_000_000_000
            )
            .is_ok(),
            "the refusals must not be reachable from an ordinary grid"
        );
    }

    /// Issue #507, W2 — **the row type's field names ARE the statement's
    /// column aliases, and the two are written in different languages.**
    ///
    /// The reader decodes with `RowBinaryWithNamesAndTypes` (validation is
    /// on by default in `pulsus_clickhouse::ChClient::query_stream`), so
    /// the database checks each returned column's NAME against the
    /// derive's `COLUMN_NAMES`. A renamed alias in `sql.rs` or a renamed
    /// field in `rows.rs` is therefore a runtime decode failure against a
    /// live database, and nothing else in the tree notices. This is the
    /// check that notices, without one.
    ///
    /// It compares the aliases the renderer emits, in order, with the
    /// derive's names — not a snapshot of the statement text, which would
    /// also move for a hundred reasons that are not this one.
    #[test]
    fn the_bucketed_row_type_is_named_for_the_statements_own_columns() {
        use crate::logql::rows::MetricRangeBucketRow;
        use pulsus_clickhouse::Row;

        let f = W0Fixtures::new();
        let sql = metric_range_bucketed(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            MetricSource::new("log_samples", MetricShape::RawCount),
            &f.one_service,
            &f.fingerprints,
            BucketedScan {
                window: TimeWindow {
                    start_ns: 1_699_999_940_000_000_000,
                    end_ns: 1_700_003_600_000_000_000,
                },
                lower: ScanLowerBound::Exclusive,
                lo_ns: 1_699_999_940_000_000_000,
                step_ns: 60_000_000_000,
            },
            &f.no_predicate,
            ScanProjection::WithStructuredMetadata,
        )
        .expect("an ordinary grid renders");

        // The SELECT list, split on TOP-LEVEL commas: the grid expression
        // carries a comma of its own inside `intDiv(...)`, so a plain
        // `split(',')` would read it as two columns.
        let select = sql
            .strip_prefix("SELECT ")
            .expect("the statement opens with its projection")
            .lines()
            .next()
            .expect("the projection is one line");
        let mut items: Vec<&str> = Vec::new();
        let mut depth = 0usize;
        let mut start = 0usize;
        for (i, c) in select.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => depth -= 1,
                ',' if depth == 0 => {
                    items.push(&select[start..i]);
                    start = i + 1;
                }
                _ => {}
            }
        }
        items.push(&select[start..]);
        // An aliased column is named by its alias; a bare one by itself.
        let names: Vec<&str> = items
            .iter()
            .map(|item| item.trim().rsplit(" AS ").next().expect("a column name"))
            .collect();

        assert_eq!(
            names,
            <MetricRangeBucketRow as Row>::COLUMN_NAMES,
            "the statement's columns and `MetricRangeBucketRow`'s fields must agree by \
             NAME and in ORDER — the decode binds on the name, and one of these two is \
             SQL text the compiler never reads"
        );
    }

    /// **T13 (issue #624, part 3a, D6): each raw read adds the `rx` column
    /// only when asked**, right after its column list; without it, the
    /// statement is today's text byte for byte.
    #[test]
    fn the_regexp_column_is_added_only_when_asked() {
        use crate::logql::predicate::regexp_captures_column;
        let f = W0Fixtures::new();
        let one = regexp_captures_column(&[super::super::pipeline::RxSource::Regexp(
            r"(?P<a>x)".to_string(),
        )])
        .expect("renders");
        let two = regexp_captures_column(&[
            super::super::pipeline::RxSource::Regexp(r"(?P<a>x)".to_string()),
            super::super::pipeline::RxSource::Regexp(r"(?P<b>[0-9]+)".to_string()),
        ])
        .expect("renders");
        assert_eq!(one.as_sql(), r"[extractGroups(body, '(?P<a>x)')]");
        assert_eq!(
            two.as_sql(),
            r"[extractGroups(body, '(?P<a>x)'), extractGroups(body, '(?P<b>[0-9]+)')]"
        );
        type Builder<'a> = Box<dyn Fn(Option<&CheckedFragment>) -> String + 'a>;
        let builders: Vec<(&str, Builder<'_>)> = vec![
            (
                "stage3",
                Box::new(|rx| {
                    stage3_with_rx(
                        &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                        "log_samples",
                        &f.one_service,
                        &f.fingerprints,
                        f.window,
                        &f.one_predicate,
                        Direction::Backward,
                        100,
                        rx,
                    )
                }),
            ),
            (
                "stage3_keyset",
                Box::new(|rx| {
                    stage3_keyset_with_rx(
                        &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                        "log_samples",
                        &f.one_service,
                        &f.fingerprints,
                        f.window,
                        KeysetLower::First,
                        Direction::Forward,
                        &f.one_predicate,
                        100,
                        rx,
                    )
                }),
            ),
            (
                "metric_raw_samples",
                Box::new(|rx| {
                    metric_raw_samples_with_rx(
                        &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                        "log_samples",
                        &f.one_service,
                        &f.fingerprints,
                        f.window,
                        ScanLowerBound::Exclusive,
                        &f.one_predicate,
                        ScanProjection::WithStructuredMetadata,
                        rx,
                    )
                }),
            ),
            (
                "metric_raw_samples_sliding",
                Box::new(|rx| {
                    metric_raw_samples_sliding_with_rx(
                        &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                        "log_samples",
                        &f.one_service,
                        &f.fingerprints,
                        f.window,
                        ScanLowerBound::Exclusive,
                        &f.one_predicate,
                        ScanProjection::WithStructuredMetadata,
                        rx,
                    )
                }),
            ),
        ];
        let todays = [
            stage3(
                &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                "log_samples",
                &f.one_service,
                &f.fingerprints,
                f.window,
                &f.one_predicate,
                Direction::Backward,
                100,
            ),
            stage3_keyset(
                &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                "log_samples",
                &f.one_service,
                &f.fingerprints,
                f.window,
                KeysetLower::First,
                Direction::Forward,
                &f.one_predicate,
                100,
            ),
            metric_raw_samples(
                &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                "log_samples",
                &f.one_service,
                &f.fingerprints,
                f.window,
                ScanLowerBound::Exclusive,
                &f.one_predicate,
                ScanProjection::WithStructuredMetadata,
            ),
            metric_raw_samples_sliding(
                &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                "log_samples",
                &f.one_service,
                &f.fingerprints,
                f.window,
                ScanLowerBound::Exclusive,
                &f.one_predicate,
                ScanProjection::WithStructuredMetadata,
            ),
        ];
        for ((name, build), today) in builders.iter().zip(todays.iter()) {
            let none = build(None);
            assert_eq!(&none, today, "{name}: no column, today's text");
            for rx in [&one, &two] {
                let want = none.replacen(
                    "\nFROM log_samples",
                    &format!(",\n       {} AS rx\nFROM log_samples", rx.as_sql()),
                    1,
                );
                assert_ne!(want, none, "{name}: the fixture has a FROM line");
                assert_eq!(build(Some(rx)), want, "{name}");
            }
        }
    }

    /// **T17 (issue #624, part 4): the run grouping is the `SELECT` that
    /// reads the table**, so over a `Distributed` table each shard groups
    /// its own lines and sends run states. The span from the last `SELECT
    /// c, kv, pk, e, lo, hi` to `GROUP BY c, kv, pk, e, lo, hi` reads the
    /// table itself, and the fan-out comes after it.
    #[test]
    fn the_run_grouping_is_the_select_that_reads_the_table() {
        let fps = [
            Fingerprint::from_raw(11).sql_literal(),
            Fingerprint::from_raw(12).sql_literal(),
        ];
        let services = [literal("checkout")];
        let pipeline = {
            let pulsus_logql::Expr::Log(le) =
                pulsus_logql::parse(r#"{a="b"} | unwrap duration(lat) | __error__="""#)
                    .expect("parse")
            else {
                panic!("a log expression");
            };
            le.pipeline
        };
        for (mode, keys) in [
            (
                KeyMode::Keep(vec!["route".to_string()]),
                ReducedKeyColumns {
                    classes: Some(vec![fps.to_vec()]),
                    hashes: vec![(fps[0], 1), (fps[1], 2)],
                    blank: vec![("route".to_string(), Vec::new())],
                    ..ReducedKeyColumns::default()
                },
            ),
            (
                KeyMode::Drop(vec!["lat".to_string(), "trace_id".to_string()]),
                ReducedKeyColumns::default(),
            ),
        ] {
            let value = ReducedUnwrap {
                reducer: pulsus_logql::RangeAggOp::QuantileOverTime,
                quantile: Some(0.9),
                label: "lat".to_string(),
                conversion: UnwrapConversion::Duration,
                mode: mode.clone(),
                key_stages: pipeline.clone(),
                todays_route: super::super::plan::ClientAgg {
                    pipeline: pipeline.clone(),
                    value: super::super::plan::ClientValue::Unwrap,
                    range_op: pulsus_logql::RangeAggOp::QuantileOverTime,
                    param: Some(0.9),
                    absent_labels: Vec::new(),
                    grouping: None,
                },
                instant: false,
                parser: None,
            };
            let sql = metric_range_reduced(
                &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                "log_samples",
                &value,
                &keys,
                &services,
                &fps,
                SLIDING_SCAN,
                &[],
                None,
            )
            .expect("a renderable statement");
            let start = sql.rfind("SELECT c, kv, pk, e, lo, hi");
            assert!(start.is_some(), "{mode:?}: no run grouping in\n{sql}");
            let start = start.unwrap_or_default();
            let end = sql[start..].find("GROUP BY c, kv, pk, e, lo, hi");
            assert!(
                end.is_some(),
                "{mode:?}: no run grouping's GROUP BY in\n{sql}"
            );
            let span = &sql[start..start + end.unwrap_or_default()];
            assert!(
                span.contains("FROM log_samples"),
                "{mode:?}: the run grouping reads the table:\n{span}"
            );
            assert!(
                !span.contains("FROM ("),
                "{mode:?}: the run grouping reads no subquery:\n{span}"
            );
            assert!(
                !sql[..start].contains("ARRAY JOIN") && sql[start..].contains("ARRAY JOIN"),
                "{mode:?}: the fan-out follows the run grouping:\n{sql}"
            );
        }
    }

    /// **T2 (issue #624, part 4b): the parser runs in the `SELECT` that
    /// reads the table.** For each parser, the level from the `WITH` before
    /// the last `SELECT c, kv, pk, e, lo, hi` to `GROUP BY c, kv, pk, e, lo,
    /// hi` reads the table itself, holds the parser's reader, and the
    /// fan-out comes after it; the `| logfmt` form raises the syntax-tree
    /// limits.
    #[test]
    fn the_parser_runs_in_the_select_that_reads_the_table() {
        let fps = [
            Fingerprint::from_raw(11).sql_literal(),
            Fingerprint::from_raw(12).sql_literal(),
        ];
        let services = [literal("checkout")];
        let route = || {
            vec![UnwrapKeyLabel {
                label: "route".to_string(),
                source: "route".to_string(),
            }]
        };
        // (pipeline, parser, the reader the level must hold)
        let parsers: [(&str, ReducedParser, &str); 6] = [
            (
                r#"{a="b"} | json | unwrap x"#,
                ReducedParser::Json {
                    form: UnwrapForm::Bare,
                    keys: route(),
                    x_path: vec!["x".to_string()],
                },
                "JSONExtractRaw(body, 'x')",
            ),
            (
                r#"{a="b"} | json x="a.b", route="route" | unwrap x"#,
                ReducedParser::Json {
                    form: UnwrapForm::Targeted,
                    keys: route(),
                    x_path: vec!["a".to_string(), "b".to_string()],
                },
                "JSONExtractRaw(body, 'a', 'b')",
            ),
            (
                r#"{a="b"} | logfmt | unwrap x"#,
                ReducedParser::Logfmt {
                    names: vec!["route".to_string()],
                    x_source: "x".to_string(),
                    x_destination: None,
                },
                "extractAll(body, ",
            ),
            (
                r#"{a="b"} | logfmt y="x", route="route" | unwrap y"#,
                ReducedParser::Logfmt {
                    names: vec!["route".to_string()],
                    x_source: "x".to_string(),
                    x_destination: Some("y".to_string()),
                },
                "extractAll(body, ",
            ),
            (
                r#"{a="b"} | regexp "x=(?P<x>\\S*) route=(?P<route>\\S+)" | unwrap x"#,
                ReducedParser::Capture {
                    parser: CaptureParser::Regexp,
                    pattern: r"x=(?P<x>\S*) route=(?P<route>\S+)".to_string(),
                    groups: vec![("route".to_string(), 2)],
                    x_index: 1,
                },
                "extractGroups(body, ",
            ),
            (
                r#"{a="b"} | pattern "<_> x=<x> route=<route>" | unwrap x"#,
                ReducedParser::Capture {
                    parser: CaptureParser::Pattern,
                    pattern: "<_> x=<x> route=<route>".to_string(),
                    groups: vec![("route".to_string(), 2)],
                    x_index: 1,
                },
                "arrayFold((acc, l, k) ->",
            ),
        ];
        for (query, parser, reader) in parsers {
            let pipeline = {
                let pulsus_logql::Expr::Log(le) = pulsus_logql::parse(query).expect("parse") else {
                    panic!("a log expression");
                };
                le.pipeline
            };
            let label = if query.contains("unwrap y") { "y" } else { "x" };
            let logfmt = matches!(parser, ReducedParser::Logfmt { .. });
            let value = ReducedUnwrap {
                reducer: pulsus_logql::RangeAggOp::MaxOverTime,
                quantile: None,
                label: label.to_string(),
                conversion: UnwrapConversion::Number,
                mode: KeyMode::Keep(vec!["route".to_string()]),
                key_stages: pipeline.clone(),
                todays_route: super::super::plan::ClientAgg {
                    pipeline,
                    value: super::super::plan::ClientValue::Unwrap,
                    range_op: pulsus_logql::RangeAggOp::MaxOverTime,
                    param: None,
                    absent_labels: Vec::new(),
                    grouping: None,
                },
                instant: false,
                parser: Some(parser),
            };
            let sql = metric_range_reduced(
                &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                "log_samples",
                &value,
                &ReducedKeyColumns::default(),
                &services,
                &fps,
                SLIDING_SCAN,
                &[],
                None,
            )
            .expect("a renderable statement");
            let select = sql.rfind("SELECT c, kv, pk, e, lo, hi");
            assert!(select.is_some(), "{query}: no run grouping in\n{sql}");
            let select = select.unwrap_or_default();
            let start = sql[..select].rfind("WITH ");
            assert!(
                start.is_some(),
                "{query}: no WITH before the run grouping in\n{sql}"
            );
            let start = start.unwrap_or_default();
            let end = sql[select..].find("GROUP BY c, kv, pk, e, lo, hi");
            assert!(
                end.is_some(),
                "{query}: no run grouping's GROUP BY in\n{sql}"
            );
            let end = select + end.unwrap_or_default();
            let level = &sql[start..end];
            assert!(
                level.contains("FROM log_samples"),
                "{query}: the run grouping reads the table:\n{level}"
            );
            assert!(
                !level.contains("FROM ("),
                "{query}: the run grouping reads no subquery:\n{level}"
            );
            assert!(
                level.contains(reader),
                "{query}: the level runs the parser ({reader}):\n{level}"
            );
            assert!(
                !sql[..start].contains("ARRAY JOIN") && sql[end..].contains("ARRAY JOIN"),
                "{query}: the fan-out follows the run grouping:\n{sql}"
            );
            if logfmt {
                assert!(
                    sql.contains("max_ast_elements = 1000000")
                        && sql.contains("max_expanded_ast_elements = 10000000"),
                    "{query}: the logfmt form raises the syntax-tree limits:\n{sql}"
                );
            }
        }
    }

    /// The text a tenant renders as, and the tenant T-U renders every
    /// statement with (issue #653).
    const TENANT_TERM: &str = "org_id = 'tenant-q'";

    /// The `WHERE` (never a `PREWHERE`) at or after byte `from` of `sql`.
    fn next_where(sql: &str, from: usize) -> Option<usize> {
        let mut at = from;
        while let Some(i) = sql[at..].find("WHERE ") {
            let pos = at + i;
            if pos < 3 || &sql[pos - 3..pos] != "PRE" {
                return Some(pos);
            }
            at = pos + "WHERE ".len();
        }
        None
    }

    /// **T-U (issue #653).** `sql` reads `table` once, and its one tenant
    /// term is the first conjunct of the `WHERE` that follows `FROM table`
    /// (and its optional `PREWHERE`), with no other `FROM` between.
    fn assert_scoped(name: &str, sql: &str, table: &str) {
        assert_eq!(
            sql.matches(TENANT_TERM).count(),
            1,
            "{name}: exactly one `{TENANT_TERM}` in\n{sql}"
        );
        let from = format!("FROM {table}");
        let at = sql.match_indices(&from).map(|(i, _)| i).find(|i| {
            sql[i + from.len()..]
                .chars()
                .next()
                .is_none_or(char::is_whitespace)
        });
        let Some(at) = at else {
            panic!("{name}: no `{from}` in\n{sql}");
        };
        let after = at + from.len();
        let Some(where_at) = next_where(sql, after) else {
            panic!("{name}: no WHERE after `{from}` in\n{sql}");
        };
        let between = sql[after..where_at].trim();
        assert!(
            !between.contains("FROM "),
            "{name}: another FROM between `{from}` and its WHERE in\n{sql}"
        );
        assert!(
            between.is_empty() || between.starts_with("PREWHERE "),
            "{name}: only a PREWHERE may stand between `{from}` and its WHERE, found \
             {between:?} in\n{sql}"
        );
        assert!(
            sql[where_at + "WHERE ".len()..].starts_with(TENANT_TERM),
            "{name}: the tenant is not the WHERE's first conjunct in\n{sql}"
        );
    }

    /// **T-U (issue #653): every log-table read carries the tenant.** Each
    /// of the twenty builders and six wrappers, rendered for `tenant-q`
    /// with representative arguments, and the bucketed and sliding
    /// builders over both of their forms.
    #[test]
    fn every_log_table_read_carries_the_tenant() {
        let t = pulsus_model::Tenant::from_header(
            Some(&http::HeaderValue::from_static("tenant-q")),
            false,
        )
        .expect("a valid tenant");
        let months = [month_literal(2026, 10)];
        let pos = [
            crate::logql::predicate::index_positive_branch("app", Some("api"), &[])
                .expect("renders"),
        ];
        let neg = [crate::logql::predicate::index_neq_branch("zone", "o")];
        let fps = active_two();
        let services = [literal("web")];
        let window = TimeWindow {
            start_ns: 1_000,
            end_ns: 2_000,
        };
        let filters = [err_line_filter()];
        let raw = MetricSource::new("log_samples", MetricShape::RawCount);
        let rollup = MetricSource::new("log_metrics_5s", MetricShape::RollupCount);
        let rx = crate::logql::predicate::regexp_group_columns(r"(?P<a>x)", &[1]).expect("renders");
        let after = KeysetLower::After {
            tuple: (1_500, Fingerprint::from_raw(7).sql_literal(), 3),
            offset: 1,
        };
        let reduced_pipeline = {
            let pulsus_logql::Expr::Log(le) =
                pulsus_logql::parse(r#"{a="b"} | unwrap duration(lat)"#).expect("parse")
            else {
                panic!("a log expression");
            };
            le.pipeline
        };
        let reduced = ReducedUnwrap {
            reducer: pulsus_logql::RangeAggOp::SumOverTime,
            quantile: None,
            label: "lat".to_string(),
            conversion: UnwrapConversion::Duration,
            mode: KeyMode::Keep(Vec::new()),
            key_stages: reduced_pipeline.clone(),
            todays_route: super::super::plan::ClientAgg {
                pipeline: reduced_pipeline,
                value: super::super::plan::ClientValue::Unwrap,
                range_op: pulsus_logql::RangeAggOp::SumOverTime,
                param: None,
                absent_labels: Vec::new(),
                grouping: None,
            },
            instant: false,
            parser: None,
        };
        let proj = ScanProjection::WithStructuredMetadata;
        let lower = ScanLowerBound::Exclusive;
        let statements: Vec<(&str, &str, String)> = vec![
            (
                "stage1",
                "log_streams_idx",
                stage1(&t, "log_streams_idx", &months, &pos, &[]),
            ),
            (
                "stage1, a negative branch",
                "log_streams_idx",
                stage1(&t, "log_streams_idx", &months, &pos, &neg),
            ),
            (
                "probe",
                "log_streams_idx",
                probe(&t, "log_streams_idx", &months, &literal("app")),
            ),
            (
                "label_names",
                "log_streams_idx",
                label_names(&t, "log_streams_idx", &months, &fps),
            ),
            (
                "label_values",
                "log_streams_idx",
                label_values(&t, "log_streams_idx", &months, &literal("app"), &fps),
            ),
            (
                "detected_labels",
                "log_streams_idx",
                detected_labels(&t, "log_streams_idx", &months, &fps),
            ),
            (
                "active_fingerprints, unscoped",
                "log_metrics_5s",
                active_fingerprints(&t, "log_metrics_5s", None, window, RES_5S),
            ),
            (
                "active_fingerprints, scoped",
                "log_metrics_5s",
                active_fingerprints(&t, "log_metrics_5s", Some(&fps), window, RES_5S),
            ),
            ("stage2", "log_streams", stage2(&t, "log_streams", &fps)),
            (
                "stage3",
                "log_samples",
                stage3(
                    &t,
                    "log_samples",
                    &services,
                    &fps,
                    window,
                    &filters,
                    Direction::Backward,
                    100,
                ),
            ),
            (
                "stage3_with_rx",
                "log_samples",
                stage3_with_rx(
                    &t,
                    "log_samples",
                    &services,
                    &fps,
                    window,
                    &filters,
                    Direction::Backward,
                    100,
                    Some(&err_line_filter()),
                ),
            ),
            (
                "stage3_keyset",
                "log_samples",
                stage3_keyset(
                    &t,
                    "log_samples",
                    &services,
                    &fps,
                    window,
                    KeysetLower::First,
                    Direction::Forward,
                    &filters,
                    100,
                ),
            ),
            (
                "stage3_keyset_with_rx",
                "log_samples",
                stage3_keyset_with_rx(
                    &t,
                    "log_samples",
                    &services,
                    &fps,
                    window,
                    after,
                    Direction::Backward,
                    &filters,
                    100,
                    None,
                ),
            ),
            (
                "log_stats_rollup",
                "log_metrics_5s",
                log_stats_rollup(&t, "log_metrics_5s", &fps, window),
            ),
            (
                "log_stats_raw",
                "log_samples",
                log_stats_raw(&t, "log_samples", &services, &fps, window, &filters),
            ),
            (
                "log_volume_rollup",
                "log_metrics_5s",
                log_volume_rollup(&t, "log_metrics_5s", &fps, window),
            ),
            (
                "log_patterns_read",
                "log_patterns",
                log_patterns_read(&t, "log_patterns", &fps, window, 60_000_000_000),
            ),
            (
                "metric_range, rollup",
                "log_metrics_5s",
                metric_range(&t, rollup, &[], &fps, window, lower, 60, &[]),
            ),
            (
                "metric_range, raw",
                "log_samples",
                metric_range(&t, raw, &services, &fps, window, lower, 60, &filters),
            ),
            (
                "metric_instant",
                "log_samples",
                metric_instant(&t, raw, &services, &fps, window, lower, &filters, proj),
            ),
            (
                "metric_raw_samples",
                "log_samples",
                metric_raw_samples(&t, "log_samples", &services, &fps, window, lower, &[], proj),
            ),
            (
                "metric_raw_samples_with_rx",
                "log_samples",
                metric_raw_samples_with_rx(
                    &t,
                    "log_samples",
                    &services,
                    &fps,
                    window,
                    lower,
                    &filters,
                    proj,
                    Some(&err_line_filter()),
                ),
            ),
            (
                "metric_raw_samples_sliding",
                "log_samples",
                metric_raw_samples_sliding(
                    &t,
                    "log_samples",
                    &services,
                    &fps,
                    window,
                    lower,
                    &[],
                    proj,
                ),
            ),
            (
                "metric_raw_samples_sliding_with_rx",
                "log_samples",
                metric_raw_samples_sliding_with_rx(
                    &t,
                    "log_samples",
                    &services,
                    &fps,
                    window,
                    lower,
                    &filters,
                    proj,
                    None,
                ),
            ),
            (
                "metric_range_bucketed",
                "log_metrics_5s",
                metric_range_bucketed(&t, rollup, &[], &fps, BUCKETED_SCAN, &[], proj)
                    .expect("renders"),
            ),
            (
                "metric_range_bucketed_with_regexp, no stage",
                "log_samples",
                metric_range_bucketed_with_regexp(
                    &t,
                    raw,
                    &services,
                    &fps,
                    BUCKETED_SCAN,
                    &filters,
                    proj,
                    None,
                )
                .expect("renders"),
            ),
            (
                "metric_range_bucketed_with_regexp, a regexp stage",
                "log_samples",
                metric_range_bucketed_with_regexp(
                    &t,
                    raw,
                    &services,
                    &fps,
                    BUCKETED_SCAN,
                    &filters,
                    proj,
                    Some(GroupExtraction::Regexp(&rx)),
                )
                .expect("renders"),
            ),
            (
                "metric_range_sliding",
                "log_samples",
                metric_range_sliding(&t, raw, &services, &fps, SLIDING_SCAN, &[]).expect("renders"),
            ),
            (
                "metric_range_sliding_with_regexp, no stage",
                "log_samples",
                metric_range_sliding_with_regexp(&t, raw, &[], &fps, SLIDING_SCAN, &filters, None)
                    .expect("renders"),
            ),
            (
                "metric_range_sliding_with_regexp, a regexp stage",
                "log_samples",
                metric_range_sliding_with_regexp(
                    &t,
                    raw,
                    &services,
                    &fps,
                    SLIDING_SCAN,
                    &filters,
                    Some(GroupExtraction::Regexp(&rx)),
                )
                .expect("renders"),
            ),
            (
                "metric_range_reduced",
                "log_samples",
                metric_range_reduced(
                    &t,
                    "log_samples",
                    &reduced,
                    &ReducedKeyColumns::default(),
                    &services,
                    &fps,
                    SLIDING_SCAN,
                    &filters,
                    None,
                )
                .expect("renders"),
            ),
        ];
        for (name, table, sql) in &statements {
            assert_scoped(name, sql, table);
        }
    }

    /// Issue #614, part 2: the rollup statement reads each bucket as two
    /// points, its start and its end, and refuses a grid off the buckets.
    #[test]
    fn the_rollup_statement_reads_each_bucket_as_two_points() {
        use crate::logql::predicate::BucketGridRefusal;
        use pulsus_model::Fingerprint;
        let header = http::HeaderValue::from_static("t1");
        let t = Tenant::from_header(Some(&header), false).expect("tenant");
        let fps = [Fingerprint::from_raw(7).sql_literal()];
        let scan = |grid_start_ns: i64, step_ns: i64, range_ns: i64, lower| SlidingScan {
            window: TimeWindow {
                start_ns: grid_start_ns - range_ns,
                end_ns: grid_start_ns + 120_000_000_000,
            },
            lower,
            grid_start_ns,
            step_ns,
            range_ns,
        };
        let ok = scan(
            600_000_000_000,
            15_000_000_000,
            60_000_000_000,
            ScanLowerBound::Exclusive,
        );
        let count = MetricSource::new("log_metrics_5s", MetricShape::RollupCount);
        let bytes = MetricSource::new("log_metrics_5s", MetricShape::RollupBytes);
        let sql = metric_range_rollup(&t, count, &fps, ok, 5_000_000_000).expect("renders");
        assert_eq!(sql, ROLLUP_COUNT_STATEMENT.join("\n"));
        let sql = metric_range_rollup(&t, bytes, &fps, ok, 5_000_000_000).expect("renders");
        assert_eq!(
            sql,
            ROLLUP_COUNT_STATEMENT
                .join("\n")
                .replace("sum(lines) AS m", "sum(line_bytes) AS m")
        );
        let raw = MetricSource::new("log_samples", MetricShape::RawCount);
        for (what, source, scan) in [
            ("raw shape", raw, ok),
            (
                "grid start",
                count,
                scan(
                    600_000_000_001,
                    15_000_000_000,
                    60_000_000_000,
                    ScanLowerBound::Exclusive,
                ),
            ),
            (
                "step",
                count,
                scan(
                    600_000_000_000,
                    3_000_000_000,
                    60_000_000_000,
                    ScanLowerBound::Exclusive,
                ),
            ),
            (
                "range",
                count,
                scan(
                    600_000_000_000,
                    15_000_000_000,
                    62_000_000_000,
                    ScanLowerBound::Exclusive,
                ),
            ),
            (
                "inclusive",
                count,
                scan(
                    600_000_000_000,
                    15_000_000_000,
                    60_000_000_000,
                    ScanLowerBound::Inclusive,
                ),
            ),
        ] {
            assert_eq!(
                metric_range_rollup(&t, source, &fps, scan, 5_000_000_000),
                Err(BucketGridRefusal::OffTheRollupGrid),
                "{what}"
            );
        }
    }

    const ROLLUP_COUNT_STATEMENT: &[&str] = &[
        "SELECT fingerprint, toInt64(600000000000 + k * 15000000000) AS bucket_ns, toUInt64(v) AS n, '' AS structured_metadata",
        "FROM (",
        "  SELECT fingerprint, k0,",
        "         sum(dv) OVER cum AS v, sum(dc) OVER cum AS p, leadInFrame(k0, 1, 9) OVER whole AS k1",
        "  FROM (",
        "    SELECT fingerprint, d.1 AS k0, sum(d.2) AS dv, sum(d.3) AS dc",
        "    FROM (",
        "      SELECT fingerprint,",
        "             greatest(intDiv(timestamp_ns - 540000000000 + 14999999999, 15000000000) - 4, 0) AS lo,",
        "             least(intDiv(timestamp_ns - 480000000000 + 14999999999, 15000000000) - 5, 8) AS hi,",
        "             sum(lines) AS m, sum(lines) AS c",
        "      FROM log_metrics_5s",
        "      ARRAY JOIN [bucket_ns + 5000000000, bucket_ns] AS timestamp_ns,",
        "                 [toInt64(count) - toInt64(count_at), toInt64(count_at)] AS lines,",
        "                 [toInt64(bytes) - toInt64(bytes_at), toInt64(bytes_at)] AS line_bytes",
        "      WHERE org_id = 't1' AND fingerprint IN (toUInt128('7'))",
        "        AND bucket_ns >= 540000000000 AND bucket_ns <= 720000000000",
        "        AND lines > 0 AND timestamp_ns > 540000000000 AND timestamp_ns <= 720000000000",
        "      GROUP BY fingerprint, lo, hi",
        "      HAVING lo <= hi",
        "    )",
        "    ARRAY JOIN [(lo, toInt64(m), toInt64(c)), (hi + 1, -toInt64(m), -toInt64(c))] AS d",
        "    GROUP BY fingerprint, k0",
        "  )",
        "  WINDOW cum AS (PARTITION BY fingerprint ORDER BY k0 ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),",
        "         whole AS (PARTITION BY fingerprint ORDER BY k0 ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
        ")",
        "ARRAY JOIN range(k0, least(k1, 9)) AS k",
        "WHERE p > 0",
    ];
}
