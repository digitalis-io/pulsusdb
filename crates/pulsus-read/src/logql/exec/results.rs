use super::*;

/// One resolved stream's response shape: labels as the raw canonical-JSON
/// string stage 2 returned (this crate parses labels only where it must —
/// vector-aggregation grouping — never to re-encode a response; #13 owns
/// the JSON envelope and already depends on a JSON crate for it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamResult {
    pub fingerprint: Fingerprint,
    pub service: String,
    /// With `categorize_labels` (issue #463) this holds the
    /// STREAM-category labels only; without it, the full final label set
    /// exactly as before.
    pub labels_json: String,
    /// `(timestamp_ns, body)`, in the plan's requested direction.
    pub entries: Vec<(i64, String)>,
    /// Parallel to [`Self::entries`] when non-empty (issue #463).
    ///
    /// **Empty on every non-categorised response**, which is why that
    /// path allocates nothing new per entry and stays byte-identical: a
    /// `StreamResult` grows one empty `Vec` per STREAM (24 B), never per
    /// entry.
    pub categories: Vec<EntryCategories>,
}

/// One entry's non-stream labels (issue #463), split the way
/// `LabelsBuilder.LabelsResult` splits them
/// (`pkg/logql/log/labels.go:606-626 @ grafana/loki v3.7.4 b318f282`).
/// Both vectors are sorted by key and neither may contain a name the
/// other does.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EntryCategories {
    /// Rendered under the entry's `structuredMetadata` key.
    pub structured_metadata: Vec<(String, String)>,
    /// Rendered under the entry's `parsed` key. Includes `__error__` /
    /// `__error_details__` — the reference files both under `parsed`
    /// unconditionally (`labels.go:610-614`), including when they
    /// arrived as structured metadata.
    pub parsed: Vec<(String, String)>,
}

/// Which `values` element shape a stream can serve (issue #463). The
/// categorised wire shape is all-or-nothing: a three-element entry in a
/// body that does not advertise `categorize-labels`, or a two-element
/// entry in one that does, is a hard parse failure in the datasource's
/// streaming decoder — so the decision is taken once, from the data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireArity {
    Two,
    Three,
}

impl StreamResult {
    /// `Three` iff [`Self::categories`] is exactly parallel to
    /// [`Self::entries`].
    ///
    /// A stream whose vectors disagree reports `Two`, so a construction
    /// bug DOWNGRADES the whole response to the two-element shape instead
    /// of emitting a body no client can parse.
    pub fn wire_arity(&self) -> WireArity {
        if !self.entries.is_empty() && self.categories.len() == self.entries.len() {
            WireArity::Three
        } else {
            WireArity::Two
        }
    }
}

/// One instant-query series.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorSample {
    pub labels: Vec<(String, String)>,
    pub value: f64,
}

/// One range-query series.
#[derive(Debug, Clone, PartialEq)]
pub struct MatrixSeries {
    pub labels: Vec<(String, String)>,
    /// `(step_ns, value)`, ascending by step.
    pub points: Vec<(i64, f64)>,
}

/// M7-A5b-i: one element's value — a plain float or a native histogram
/// (`FloatHistogram`, the eval-result type). Additive companion to
/// [`VectorSample`]/[`MatrixSeries`] (which stay float-only, LogQL/traces-
/// shared, untouched) — a metrics query whose result carries at least one
/// histogram-valued element/point routes through [`QueryResult::VectorHist`]/
/// [`QueryResult::MatrixHist`] instead.
#[derive(Debug, Clone)]
pub enum HistOrFloat {
    Float(f64),
    Hist(Box<pulsus_model::FloatHistogram>),
}

/// Hand-written (no `PartialEq` derive on `FloatHistogram` — NaN-bearing
/// fields, `pulsus_model`'s own doc): float arm via native `f64::eq`
/// (`NaN != NaN`), histogram arm via `FloatHistogram::bits_eq`, mirroring
/// `pulsus-promql::value`'s `Sample`/`Point` contract.
impl PartialEq for HistOrFloat {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Float(a), Self::Float(b)) => a == b,
            (Self::Hist(a), Self::Hist(b)) => a.bits_eq(b),
            _ => false,
        }
    }
}

/// One instant-query series whose value may be a native histogram.
#[derive(Debug, Clone, PartialEq)]
pub struct HistVectorSample {
    pub labels: Vec<(String, String)>,
    pub value: HistOrFloat,
}

/// One range-query series whose points may mix float and histogram values
/// (a series's underlying sample type can change mid-window).
#[derive(Debug, Clone, PartialEq)]
pub struct HistMatrixSeries {
    pub labels: Vec<(String, String)>,
    /// `(step_ns, value)`, ascending by step.
    pub points: Vec<(i64, HistOrFloat)>,
}

/// The engine's raw result — #13 encodes this into the query-API JSON
/// envelope (out of scope here per the architect plan). `Scalar` is issue
/// #31's addition (`pulsus_promql::QueryValue::Scalar` — a bare-number
/// PromQL expression, e.g. `1 + 1`, evaluated with no series involved);
/// LogQL never produces it.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryResult {
    /// Log-line streams. `partial` (issue #90) signals a budget-exhausted
    /// fetch-until-limit result: the paging loop stopped because the byte
    /// scan budget was spent, not because the window ran out of matching
    /// lines. The encoder surfaces it as `stats.pulsus_partial`
    /// (skip-if-false, so ordinary responses stay byte-identical). Always
    /// `false` on the fast/non-dropping paths and on genuine exhaustion.
    Streams {
        items: Vec<StreamResult>,
        partial: bool,
    },
    Vector(Vec<VectorSample>),
    Matrix(Vec<MatrixSeries>),
    Scalar(f64),
    /// A top-level PromQL string-literal query (issue #86, M6-08d) —
    /// `pulsus_promql::QueryValue::String`, rendered by the prom API as
    /// `resultType:"string"`. Like [`QueryResult::Scalar`], the wire
    /// timestamp is stamped externally by the encoder from the request's
    /// evaluation time (`at_ms`), never carried in the variant. LogQL
    /// never produces it.
    String(String),
    /// M7-A5b-i: an instant-query result carrying at least one histogram-
    /// valued element (metrics-only — replaces the A5a
    /// `HistogramResultUnsupported` reject).
    VectorHist(Vec<HistVectorSample>),
    /// M7-A5b-i: a range-query result carrying at least one histogram-
    /// valued point (metrics-only).
    MatrixHist(Vec<HistMatrixSeries>),
}
