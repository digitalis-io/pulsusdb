//! Strict parsers for every answer the profileql_frontend scenario judges
//! (issue #832). Each returns an error on any missing, mistyped or
//! malformed part; none skips, defaults or coerces. A check that asks for a
//! parse and gets an error fails with it.

use std::collections::BTreeMap;

use serde_json::{Map, Value};

pub(crate) type Parsed<T> = Result<T, String>;

// ---------------------------------------------------------------------
// JSON primitives
// ---------------------------------------------------------------------

pub(crate) fn json(text: &str) -> Parsed<Value> {
    serde_json::from_str(text).map_err(|e| format!("not JSON ({e}): {text:.200}"))
}

fn object<'a>(v: &'a Value, what: &str) -> Parsed<&'a Map<String, Value>> {
    v.as_object()
        .ok_or_else(|| format!("{what} is not an object: {v:.200}"))
}

fn array<'a>(v: &'a Value, what: &str) -> Parsed<&'a Vec<Value>> {
    v.as_array()
        .ok_or_else(|| format!("{what} is not an array: {v:.200}"))
}

fn get<'a>(o: &'a Map<String, Value>, key: &str, what: &str) -> Parsed<&'a Value> {
    o.get(key).ok_or_else(|| format!("{what} has no {key:?}"))
}

fn string<'a>(v: &'a Value, what: &str) -> Parsed<&'a str> {
    v.as_str()
        .ok_or_else(|| format!("{what} is not a string: {v:.200}"))
}

/// A JSON integer; a float, a string or anything else is an error.
fn integer(v: &Value, what: &str) -> Parsed<i64> {
    v.as_i64()
        .ok_or_else(|| format!("{what} is not an integer: {v:.200}"))
}

/// An int64 as proto3 JSON writes it: a decimal string.
fn int64_string(v: &Value, what: &str) -> Parsed<i64> {
    string(v, what)?
        .parse()
        .map_err(|e| format!("{what} is not an int64 string ({e}): {v}"))
}

fn number(v: &Value, what: &str) -> Parsed<f64> {
    v.as_f64()
        .ok_or_else(|| format!("{what} is not a number: {v:.200}"))
}

// ---------------------------------------------------------------------
// The query answer, its frames and fields
// ---------------------------------------------------------------------

/// One field of a data frame, with its column.
#[derive(Debug, Clone)]
pub(crate) struct Field {
    pub name: String,
    pub kind: String,
    pub labels: BTreeMap<String, String>,
    pub config: Value,
    pub values: Vec<Value>,
}

/// One data frame: its name, its visualisation hint, and its fields, every
/// column the same length.
#[derive(Debug, Clone)]
pub(crate) struct Frame {
    pub name: String,
    pub visualisation: Option<String>,
    pub fields: Vec<Field>,
    pub rows: usize,
}

/// One result of a query answer: status 200, no error, its frames.
#[derive(Debug, Clone)]
pub(crate) struct QueryResult {
    pub frames: Vec<Frame>,
}

/// `/api/ds/query`'s answer: every result by its `refId`, at least one.
pub(crate) fn answer(text: &str) -> Parsed<BTreeMap<String, QueryResult>> {
    let body = json(text)?;
    let results = object(
        get(object(&body, "the answer")?, "results", "the answer")?,
        "results",
    )?;
    if results.is_empty() {
        return Err("the answer has no results".to_string());
    }
    results
        .iter()
        .map(|(ref_id, r)| {
            Ok((
                ref_id.clone(),
                result(r).map_err(|e| format!("result {ref_id}: {e}"))?,
            ))
        })
        .collect()
}

/// One result: exactly status 200, no `error`, and a `frames` array.
pub(crate) fn result(v: &Value) -> Parsed<QueryResult> {
    let o = object(v, "the result")?;
    if let Some(error) = o.get("error") {
        return Err(format!("error {error}"));
    }
    let status = integer(get(o, "status", "the result")?, "status")?;
    if status != 200 {
        return Err(format!("status {status}"));
    }
    let frames = array(get(o, "frames", "the result")?, "frames")?
        .iter()
        .enumerate()
        .map(|(i, f)| frame(f).map_err(|e| format!("frame {i}: {e}")))
        .collect::<Parsed<Vec<_>>>()?;
    Ok(QueryResult { frames })
}

/// One data frame: `schema.name`, `schema.fields` and `data.values`, one
/// column per field, every column the same length.
pub(crate) fn frame(v: &Value) -> Parsed<Frame> {
    let o = object(v, "the frame")?;
    let schema = object(get(o, "schema", "the frame")?, "schema")?;
    let name = string(get(schema, "name", "schema")?, "schema.name")?.to_string();
    let visualisation = match schema.get("meta") {
        None => None,
        Some(meta) => match object(meta, "schema.meta")?.get("preferredVisualisationType") {
            None => None,
            Some(t) => Some(string(t, "preferredVisualisationType")?.to_string()),
        },
    };
    let specs = array(get(schema, "fields", "schema")?, "schema.fields")?;
    let data = object(get(o, "data", "the frame")?, "data")?;
    let columns = array(get(data, "values", "data")?, "data.values")?;
    if columns.len() != specs.len() {
        return Err(format!(
            "{} fields but {} columns",
            specs.len(),
            columns.len()
        ));
    }
    let mut fields = Vec::with_capacity(specs.len());
    for (i, (spec, column)) in specs.iter().zip(columns).enumerate() {
        let what = format!("field {i}");
        let s = object(spec, &what)?;
        let labels = match s.get("labels") {
            None => BTreeMap::new(),
            Some(l) => object(l, &format!("{what} labels"))?
                .iter()
                .map(|(k, v)| {
                    Ok((
                        k.clone(),
                        string(v, &format!("{what} label {k}"))?.to_string(),
                    ))
                })
                .collect::<Parsed<_>>()?,
        };
        fields.push(Field {
            name: string(get(s, "name", &what)?, &format!("{what} name"))?.to_string(),
            kind: string(get(s, "type", &what)?, &format!("{what} type"))?.to_string(),
            labels,
            config: s.get("config").cloned().unwrap_or(Value::Null),
            values: array(column, &format!("{what} column"))?.clone(),
        });
    }
    let rows = fields.first().map_or(0, |f| f.values.len());
    if let Some(f) = fields.iter().find(|f| f.values.len() != rows) {
        return Err(format!(
            "column {} has {} values, column {} has {rows}",
            f.name,
            f.values.len(),
            fields[0].name
        ));
    }
    Ok(Frame {
        name,
        visualisation,
        fields,
        rows,
    })
}

impl Frame {
    /// The fields' names and types, exactly these, in this order.
    fn require(&self, want: &[(&str, &str)]) -> Parsed<()> {
        let got: Vec<(&str, &str)> = self
            .fields
            .iter()
            .map(|f| (f.name.as_str(), f.kind.as_str()))
            .collect();
        if got != want {
            return Err(format!(
                "frame {}: fields {got:?}, expected {want:?}",
                self.name
            ));
        }
        Ok(())
    }

    fn numbers(&self, i: usize) -> Parsed<Vec<f64>> {
        let f = &self.fields[i];
        f.values
            .iter()
            .map(|v| number(v, &format!("{} value", f.name)))
            .collect()
    }

    fn integers(&self, i: usize) -> Parsed<Vec<i64>> {
        let f = &self.fields[i];
        f.values
            .iter()
            .map(|v| integer(v, &format!("{} value", f.name)))
            .collect()
    }
}

// ---------------------------------------------------------------------
// The kinds of frame the profiles datasource answers
// ---------------------------------------------------------------------

/// A flame graph: `level`, `value`, `self` and `label`, integers and enum
/// indices, at least the root, which is `total` at level 0.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FlameGraph {
    pub levels: Vec<i64>,
    pub values: Vec<i64>,
    pub selfs: Vec<i64>,
    pub labels: Vec<String>,
}

impl FlameGraph {
    pub(crate) fn root(&self) -> i64 {
        self.values[0]
    }

    /// The empty profile: the root alone, at 0.
    pub(crate) fn is_empty_profile(&self) -> bool {
        self.levels == [0] && self.values == [0] && self.selfs == [0] && self.labels == ["total"]
    }
}

pub(crate) fn flame(f: &Frame) -> Parsed<FlameGraph> {
    if f.name != "response" || f.visualisation.as_deref() != Some("flamegraph") {
        return Err(format!(
            "frame {} ({:?}) is not a flame graph",
            f.name, f.visualisation
        ));
    }
    f.require(&[
        ("level", "number"),
        ("value", "number"),
        ("self", "number"),
        ("label", "enum"),
    ])?;
    if f.rows == 0 {
        return Err("the flame graph has no nodes".to_string());
    }
    let names = array(
        &f.fields[3].config["type"]["enum"]["text"],
        "the label enum",
    )?
    .iter()
    .map(|n| string(n, "a label name").map(String::from))
    .collect::<Parsed<Vec<_>>>()?;
    let labels = f
        .integers(3)?
        .into_iter()
        .map(|i| {
            usize::try_from(i)
                .ok()
                .and_then(|i| names.get(i).cloned())
                .ok_or_else(|| format!("label index {i} outside {names:?}"))
        })
        .collect::<Parsed<Vec<_>>>()?;
    let g = FlameGraph {
        levels: f.integers(0)?,
        values: f.integers(1)?,
        selfs: f.integers(2)?,
        labels,
    };
    if g.levels[0] != 0 || g.labels[0] != "total" {
        return Err(format!(
            "the root is {:?} at level {}, not total at 0",
            g.labels[0], g.levels[0]
        ));
    }
    Ok(g)
}

/// A time series: `time` and one number field carrying the series' labels.
#[derive(Debug, Clone)]
pub(crate) struct Series {
    pub labels: BTreeMap<String, String>,
    pub times: Vec<i64>,
    pub values: Vec<f64>,
}

pub(crate) fn series(f: &Frame) -> Parsed<Series> {
    if f.name != "series" || f.fields.len() != 2 {
        return Err(format!("frame {} is not a time series", f.name));
    }
    if f.fields[0].name != "time" || f.fields[0].kind != "time" || f.fields[1].kind != "number" {
        return Err(format!("frame {}: fields are not time and number", f.name));
    }
    Ok(Series {
        labels: f.fields[1].labels.clone(),
        times: f.integers(0)?,
        values: f.fields[1]
            .values
            .iter()
            .map(|v| number(v, "a series value"))
            .collect::<Parsed<_>>()?,
    })
}

/// A heatmap: its cells' counts.
pub(crate) fn heatmap(f: &Frame) -> Parsed<Vec<i64>> {
    if f.name != "heatmap" {
        return Err(format!("frame {} is not a heatmap", f.name));
    }
    f.require(&[
        ("xMax", "time"),
        ("yMin", "number"),
        ("yMax", "number"),
        ("count", "number"),
    ])?;
    f.integers(0)?;
    let y_min = f.numbers(1)?;
    let y_max = f.numbers(2)?;
    if let Some(i) = (0..f.rows).find(|&i| y_min[i] >= y_max[i]) {
        return Err(format!(
            "cell {i}: yMin {} is not below yMax {}",
            y_min[i], y_max[i]
        ));
    }
    let counts = f.integers(3)?;
    if let Some(c) = counts.iter().find(|c| **c < 0) {
        return Err(format!("a negative count {c}"));
    }
    Ok(counts)
}

/// An exemplar frame: its row count, its first fields `Time`, `Value`, `Id`.
pub(crate) fn exemplars(f: &Frame) -> Parsed<usize> {
    if f.name != "exemplar" {
        return Err(format!("frame {} is not an exemplar frame", f.name));
    }
    let head: Vec<(&str, &str)> = f
        .fields
        .iter()
        .take(3)
        .map(|x| (x.name.as_str(), x.kind.as_str()))
        .collect();
    if head != [("Time", "time"), ("Value", "number"), ("Id", "string")] {
        return Err(format!("exemplar fields start {head:?}"));
    }
    f.integers(0)?;
    f.numbers(1)?;
    // `Id` is the profile id; the rest are the exemplar's labels.
    for (i, field) in f.fields.iter().enumerate().skip(2) {
        if field.kind != "string" {
            return Err(format!(
                "exemplar field {} is {}, not a string",
                field.name, field.kind
            ));
        }
        for v in &field.values {
            let text = string(v, &format!("exemplar {}", field.name))?;
            if i == 2 && text.is_empty() {
                return Err("an exemplar with an empty profile id".to_string());
            }
        }
    }
    Ok(f.rows)
}

/// The one frame of a result, a flame graph: a profile query's answer.
pub(crate) fn only_flame(r: &QueryResult) -> Parsed<FlameGraph> {
    match &r.frames[..] {
        [f] => flame(f),
        frames => Err(format!("{} frames, expected one flame graph", frames.len())),
    }
}

/// A frame of any kind the profiles datasource answers, parsed as that
/// kind. A frame of another name is an error.
#[derive(Debug, Clone)]
pub(crate) enum Kind {
    Flame(FlameGraph),
    Series,
    Heatmap(Vec<i64>),
    Exemplars(usize),
}

pub(crate) fn kind(f: &Frame) -> Parsed<Kind> {
    match f.name.as_str() {
        "response" => flame(f).map(Kind::Flame),
        "series" => series(f).map(|_| Kind::Series),
        "heatmap" => heatmap(f).map(Kind::Heatmap),
        "exemplar" => exemplars(f).map(Kind::Exemplars),
        other => Err(format!("unknown frame {other:?}")),
    }
}

// ---------------------------------------------------------------------
// The Connect and HTTP answers the app reads
// ---------------------------------------------------------------------

/// `Series`' label sets.
pub(crate) fn series_answer(text: &str) -> Parsed<Vec<BTreeMap<String, String>>> {
    let body = json(text)?;
    array(
        get(
            object(&body, "the Series answer")?,
            "labelsSet",
            "the Series answer",
        )?,
        "labelsSet",
    )?
    .iter()
    .map(|set| {
        array(
            get(object(set, "a label set")?, "labels", "a label set")?,
            "labels",
        )?
        .iter()
        .map(|pair| {
            let p = object(pair, "a label")?;
            Ok((
                string(get(p, "name", "a label")?, "a label name")?.to_string(),
                string(get(p, "value", "a label")?, "a label value")?.to_string(),
            ))
        })
        .collect()
    })
    .collect()
}

/// `SelectHeatmap`: every slot's counts summed, and every exemplar's span id
/// and value. A slot with profiles lists their exemplars; a slot without
/// lists none.
pub(crate) fn heatmap_answer(text: &str) -> Parsed<(i64, Vec<(String, i64)>)> {
    let body = json(text)?;
    let series = array(
        get(object(&body, "the heatmap")?, "series", "the heatmap")?,
        "series",
    )?;
    if series.is_empty() {
        return Err("the heatmap has no series".to_string());
    }
    let mut count = 0;
    let mut exemplars = Vec::new();
    for s in series {
        for slot in array(get(object(s, "a series")?, "slots", "a series")?, "slots")? {
            let o = object(slot, "a slot")?;
            int64_string(get(o, "timestamp", "a slot")?, "a slot timestamp")?;
            let y = array(get(o, "yMin", "a slot")?, "yMin")?;
            let counts = array(get(o, "counts", "a slot")?, "counts")?;
            if y.len() != counts.len() {
                return Err(format!("{} buckets but {} counts", y.len(), counts.len()));
            }
            let starts = y
                .iter()
                .map(|v| number(v, "a bucket start"))
                .collect::<Parsed<Vec<f64>>>()?;
            if starts.windows(2).any(|w| w[0] >= w[1]) {
                return Err(format!("bucket starts not ascending: {starts:?}"));
            }
            let counts = counts
                .iter()
                .map(|c| integer(c, "a count"))
                .collect::<Parsed<Vec<i64>>>()?;
            if counts.iter().any(|c| *c < 0) {
                return Err(format!("a negative count in {counts:?}"));
            }
            let slot_count: i64 = counts.iter().sum();
            count += slot_count;
            match (o.get("exemplars"), slot_count) {
                (None, 0) => {}
                (None, _) => return Err("a slot with profiles has no exemplars".to_string()),
                (Some(list), _) => {
                    for e in array(list, "exemplars")? {
                        exemplars.push(heatmap_exemplar(e)?);
                    }
                }
            }
        }
    }
    Ok((count, exemplars))
}

/// One heatmap exemplar: its time, its span id (16 lowercase hex digits),
/// its value, its trace id when it has one (32), and its labels.
fn heatmap_exemplar(v: &Value) -> Parsed<(String, i64)> {
    let e = object(v, "an exemplar")?;
    int64_string(get(e, "timestamp", "an exemplar")?, "an exemplar timestamp")?;
    let span = string(get(e, "spanId", "an exemplar")?, "spanId")?;
    hex_id(span, 16, "spanId")?;
    let value = int64_string(get(e, "value", "an exemplar")?, "an exemplar value")?;
    if let Some(trace) = e.get("traceId") {
        hex_id(string(trace, "traceId")?, 32, "traceId")?;
    }
    if let Some(labels) = e.get("labels") {
        for pair in array(labels, "exemplar labels")? {
            let p = object(pair, "an exemplar label")?;
            string(get(p, "name", "an exemplar label")?, "a label name")?;
            string(get(p, "value", "an exemplar label")?, "a label value")?;
        }
    }
    Ok((span.to_string(), value))
}

fn hex_id(s: &str, len: usize, what: &str) -> Parsed<()> {
    if s.len() == len
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        Ok(())
    } else {
        Err(format!("{what} {s:?} is not {len} lowercase hex digits"))
    }
}

/// `render-diff`: both sides' totals, which the flame graph's total is.
pub(crate) fn diff_answer(text: &str) -> Parsed<(i64, i64)> {
    let body = json(text)?;
    let o = object(&body, "the diff")?;
    let left = integer(get(o, "leftTicks", "the diff")?, "leftTicks")?;
    let right = integer(get(o, "rightTicks", "the diff")?, "rightTicks")?;
    let fb = object(get(o, "flamebearer", "the diff")?, "flamebearer")?;
    let total = integer(get(fb, "numTicks", "flamebearer")?, "numTicks")?;
    if total != left + right {
        return Err(format!("numTicks {total} is not {left} + {right}"));
    }
    Ok((left, right))
}

/// The ad hoc upload's flame graph total.
pub(crate) fn upload_answer(text: &str) -> Parsed<i64> {
    let body = json(text)?;
    let o = object(&body, "the upload")?;
    let profile = json(string(
        get(o, "flamebearerProfile", "the upload")?,
        "flamebearerProfile",
    )?)?;
    let fb = object(
        get(
            object(&profile, "the profile")?,
            "flamebearer",
            "the profile",
        )?,
        "flamebearer",
    )?;
    integer(get(fb, "numTicks", "flamebearer")?, "numTicks")
}

/// Settings `Get`: `pluginSettings`' `maxNodes`, an integer.
pub(crate) fn max_nodes(text: &str) -> Parsed<i64> {
    let settings = plugin_settings(text)?;
    integer(
        get(
            object(&settings, "pluginSettings")?,
            "maxNodes",
            "pluginSettings",
        )?,
        "maxNodes",
    )
}

/// Settings `Get`: the app's settings, `pluginSettings`, as JSON.
fn plugin_settings(text: &str) -> Parsed<Value> {
    let body = json(text)?;
    let all = array(
        get(object(&body, "the settings")?, "settings", "the settings")?,
        "settings",
    )?;
    let mut found = Vec::new();
    for s in all {
        let o = object(s, "a setting")?;
        if string(get(o, "name", "a setting")?, "a setting name")? == "pluginSettings" {
            found.push(json(string(
                get(o, "value", "a setting")?,
                "a setting value",
            )?)?);
        }
    }
    match <[Value; 1]>::try_from(found) {
        Ok([v]) => Ok(v),
        Err(found) => Err(format!("{} pluginSettings entries", found.len())),
    }
}

/// A Connect error body's `code`.
pub(crate) fn connect_code(text: &str) -> Parsed<String> {
    let body = json(text)?;
    Ok(string(
        get(object(&body, "the error")?, "code", "the error")?,
        "code",
    )?
    .to_string())
}

/// One query of an `/api/ds/query` request: its `refId`, and the profile
/// fields when it has them. A field present with the wrong type is an
/// error; a field absent is `None`, a query of another kind.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RequestQuery {
    pub ref_id: String,
    pub span_selector: Option<Vec<String>>,
    pub label_selector: Option<String>,
    pub profile_type_id: Option<String>,
}

/// An `/api/ds/query` request's queries.
pub(crate) fn request_queries(text: &str) -> Parsed<Vec<RequestQuery>> {
    let body = json(text)?;
    array(
        get(object(&body, "the request")?, "queries", "the request")?,
        "queries",
    )?
    .iter()
    .map(|q| {
        let o = object(q, "a query")?;
        let opt_string = |key: &str| -> Parsed<Option<String>> {
            o.get(key)
                .map(|v| string(v, key).map(String::from))
                .transpose()
        };
        Ok(RequestQuery {
            ref_id: string(get(o, "refId", "a query")?, "refId")?.to_string(),
            span_selector: o
                .get("spanSelector")
                .map(|v| {
                    array(v, "spanSelector")?
                        .iter()
                        .map(|s| string(s, "a span id").map(String::from))
                        .collect::<Parsed<Vec<_>>>()
                })
                .transpose()?,
            label_selector: opt_string("labelSelector")?,
            profile_type_id: opt_string("profileTypeId")?,
        })
    })
    .collect()
}

/// The trace answer's span count, over every resource and scope.
pub(crate) fn trace_span_count(text: &str) -> Parsed<usize> {
    let body = json(text)?;
    let o = object(&body, "the trace")?;
    let resources = match (o.get("resourceSpans"), o.get("batches")) {
        (Some(r), None) | (None, Some(r)) => array(r, "the resource spans")?,
        _ => return Err("the trace has neither or both of resourceSpans and batches".to_string()),
    };
    let mut n = 0;
    for r in resources {
        for scope in array(
            get(object(r, "a resource")?, "scopeSpans", "a resource")?,
            "scopeSpans",
        )? {
            n += array(get(object(scope, "a scope")?, "spans", "a scope")?, "spans")?.len();
        }
    }
    Ok(n)
}

/// A plugin's settings: its version and whether it is enabled.
pub(crate) fn plugin_info(text: &str) -> Parsed<(String, bool)> {
    let body = json(text)?;
    let o = object(&body, "the plugin")?;
    let info = object(get(o, "info", "the plugin")?, "info")?;
    let version = string(get(info, "version", "info")?, "version")?.to_string();
    let enabled = get(o, "enabled", "the plugin")?
        .as_bool()
        .ok_or("enabled is not a boolean")?;
    Ok((version, enabled))
}

/// A datasource's `uid`.
pub(crate) fn datasource_uid(text: &str) -> Parsed<String> {
    let body = json(text)?;
    Ok(string(
        get(object(&body, "the datasource")?, "uid", "the datasource")?,
        "uid",
    )?
    .to_string())
}

/// The browser script's report: its own typed shape, every field required.
pub(crate) fn pages(text: &str) -> Parsed<Vec<super::PageRecord>> {
    serde_json::from_str(text).map_err(|e| format!("the browser's report ({e}): {text:.500}"))
}

/// The datasource's health: exactly `OK`.
pub(crate) fn health(text: &str) -> Parsed<()> {
    let body = json(text)?;
    match string(
        get(object(&body, "the health")?, "status", "the health")?,
        "status",
    )? {
        "OK" => Ok(()),
        other => Err(format!("health {other}: {body}")),
    }
}

/// A list of strings, every element a string.
pub(crate) fn strings(text: &str) -> Parsed<Vec<String>> {
    let body = json(text)?;
    array(&body, "the list")?
        .iter()
        .map(|v| string(v, "a list element").map(String::from))
        .collect()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use serde_json::json;

    /// A flame graph frame as the datasource writes it: `total` at `root`
    /// over one child.
    pub(crate) fn flame_frame(root: i64) -> Value {
        json!({
            "schema": {"name": "response", "refId": "A",
                       "meta": {"preferredVisualisationType": "flamegraph"},
                       "fields": [{"name": "level", "type": "number"},
                                  {"name": "value", "type": "number"},
                                  {"name": "self", "type": "number"},
                                  {"name": "label", "type": "enum",
                                   "config": {"type": {"enum": {"text": ["total", "main"]}}}}]},
            "data": {"values": [[0, 1], [root, root], [0, root], [0, 1]]},
        })
    }

    fn empty_frame() -> Value {
        json!({
            "schema": {"name": "response",
                       "meta": {"preferredVisualisationType": "flamegraph"},
                       "fields": [{"name": "level", "type": "number"},
                                  {"name": "value", "type": "number"},
                                  {"name": "self", "type": "number"},
                                  {"name": "label", "type": "enum",
                                   "config": {"type": {"enum": {"text": ["total"]}}}}]},
            "data": {"values": [[0], [0], [0], [0]]},
        })
    }

    fn series_frame() -> Value {
        json!({
            "schema": {"name": "series",
                       "fields": [{"name": "time", "type": "time"},
                                  {"name": "cpu", "type": "number", "labels": {"service_name": "svc-a"}}]},
            "data": {"values": [[1, 2], [0.5, 0.25]]},
        })
    }

    fn heatmap_frame() -> Value {
        json!({
            "schema": {"name": "heatmap",
                       "fields": [{"name": "xMax", "type": "time"}, {"name": "yMin", "type": "number"},
                                  {"name": "yMax", "type": "number"}, {"name": "count", "type": "number"}]},
            "data": {"values": [[1], [0.0], [1.0], [3]]},
        })
    }

    fn exemplar_frame() -> Value {
        json!({
            "schema": {"name": "exemplar",
                       "fields": [{"name": "Time", "type": "time"}, {"name": "Value", "type": "number"},
                                  {"name": "Id", "type": "string"}]},
            "data": {"values": [[1, 2], [0.5, 0.5], ["a", "b"]]},
        })
    }

    fn answer_of(frames: Vec<Value>) -> Value {
        json!({"results": {"A": {"status": 200, "frames": frames}}})
    }

    type Break<'a> = (&'a str, &'a dyn Fn(&mut Value));

    /// `good` parses with `parse`; every `(name, change)` applied to a copy
    /// of it makes `parse` fail.
    fn each_break<T: std::fmt::Debug>(
        good: Value,
        parse: impl Fn(&str) -> Parsed<T>,
        breaks: &[Break<'_>],
    ) {
        if let Err(e) = parse(&good.to_string()) {
            panic!("the good case does not parse: {e}");
        }
        for (name, change) in breaks {
            let mut v = good.clone();
            change(&mut v);
            let got = parse(&v.to_string());
            assert!(got.is_err(), "{name}: parsed as {got:?}");
        }
    }

    fn frame_text(text: &str) -> Parsed<Frame> {
        frame(&json(text)?)
    }

    fn flame_text(text: &str) -> Parsed<FlameGraph> {
        flame(&frame(&json(text)?)?)
    }

    #[test]
    fn the_query_answer_refuses_every_malformed_part() {
        each_break(
            answer_of(vec![flame_frame(5)]),
            answer,
            &[
                ("results missing", &|v| v["results"] = Value::Null),
                ("results empty", &|v| v["results"] = json!({})),
                ("status missing", &|v| {
                    v["results"]["A"].as_object_mut().unwrap().remove("status");
                }),
                ("status a string", &|v| {
                    v["results"]["A"]["status"] = json!("200")
                }),
                ("status 300", &|v| v["results"]["A"]["status"] = json!(300)),
                ("status 500 with no error", &|v| {
                    v["results"]["A"]["status"] = json!(500)
                }),
                ("an error field", &|v| {
                    v["results"]["A"]["error"] = json!("boom")
                }),
                ("frames missing", &|v| {
                    v["results"]["A"].as_object_mut().unwrap().remove("frames");
                }),
                ("frames not an array", &|v| {
                    v["results"]["A"]["frames"] = json!({})
                }),
                ("one bad result among good ones", &|v| {
                    v["results"]["B"] = v["results"]["A"].clone();
                    v["results"]["C"] = json!({"status": 404, "frames": []});
                }),
                ("one bad frame among good ones", &|v| {
                    let bad = json!({"schema": {"name": "series"}});
                    v["results"]["A"]["frames"]
                        .as_array_mut()
                        .unwrap()
                        .push(bad);
                }),
            ],
        );
        assert!(answer("not json").is_err());
    }

    #[test]
    fn a_frame_refuses_every_malformed_part() {
        each_break(
            series_frame(),
            frame_text,
            &[
                ("schema missing", &|v| {
                    v.as_object_mut().unwrap().remove("schema");
                }),
                ("name missing", &|v| {
                    v["schema"].as_object_mut().unwrap().remove("name");
                }),
                ("name a number", &|v| v["schema"]["name"] = json!(1)),
                ("meta not an object", &|v| v["schema"]["meta"] = json!(1)),
                ("fields not an array", &|v| {
                    v["schema"]["fields"] = json!({})
                }),
                ("data missing", &|v| {
                    v.as_object_mut().unwrap().remove("data");
                }),
                ("one column short", &|v| {
                    v["data"]["values"].as_array_mut().unwrap().pop();
                }),
                ("a column not an array", &|v| {
                    v["data"]["values"][1] = json!(0.5)
                }),
                ("columns of different lengths", &|v| {
                    v["data"]["values"][1] = json!([0.5])
                }),
                ("a field without a name", &|v| {
                    v["schema"]["fields"][1]
                        .as_object_mut()
                        .unwrap()
                        .remove("name");
                }),
                ("a field type a number", &|v| {
                    v["schema"]["fields"][1]["type"] = json!(3)
                }),
                ("a label value a number", &|v| {
                    v["schema"]["fields"][1]["labels"]["service_name"] = json!(1)
                }),
            ],
        );
    }

    #[test]
    fn a_flame_graph_refuses_every_malformed_part() {
        each_break(
            flame_frame(5),
            flame_text,
            &[
                ("another frame name", &|v| {
                    v["schema"]["name"] = json!("series")
                }),
                ("no flamegraph hint", &|v| {
                    v["schema"].as_object_mut().unwrap().remove("meta");
                }),
                ("a field missing", &|v| {
                    v["schema"]["fields"].as_array_mut().unwrap().remove(2);
                    v["data"]["values"].as_array_mut().unwrap().remove(2);
                }),
                ("fields out of order", &|v| {
                    v["schema"]["fields"].as_array_mut().unwrap().swap(1, 2)
                }),
                ("a field of the wrong type", &|v| {
                    v["schema"]["fields"][3]["type"] = json!("string")
                }),
                ("level \"bad\"", &|v| {
                    v["data"]["values"][0][1] = json!("bad")
                }),
                ("self \"bad\"", &|v| {
                    v["data"]["values"][2][0] = json!("bad")
                }),
                ("value a float", &|v| v["data"]["values"][1][0] = json!(5.5)),
                ("value a string", &|v| {
                    v["data"]["values"][1][0] = json!("5")
                }),
                ("no nodes", &|v| {
                    v["data"]["values"] = json!([[], [], [], []])
                }),
                ("a value column short", &|v| {
                    v["data"]["values"][1] = json!([5])
                }),
                ("a label index outside the names", &|v| {
                    v["data"]["values"][3][1] = json!(7)
                }),
                ("a label index negative", &|v| {
                    v["data"]["values"][3][1] = json!(-1)
                }),
                ("the label names missing", &|v| {
                    v["schema"]["fields"][3]
                        .as_object_mut()
                        .unwrap()
                        .remove("config");
                }),
                ("a label name a number", &|v| {
                    v["schema"]["fields"][3]["config"]["type"]["enum"]["text"][1] = json!(2)
                }),
                ("the root not total", &|v| {
                    v["data"]["values"][3][0] = json!(1)
                }),
                ("the root not at level 0", &|v| {
                    v["data"]["values"][0][0] = json!(1)
                }),
            ],
        );
        assert_eq!(flame_text(&flame_frame(5).to_string()).unwrap().root(), 5);
    }

    #[test]
    fn the_empty_profile_is_only_the_root_alone_at_zero() {
        assert!(
            flame_text(&empty_frame().to_string())
                .unwrap()
                .is_empty_profile()
        );
        assert!(
            !flame_text(&flame_frame(0).to_string())
                .unwrap()
                .is_empty_profile()
        );
        let mut v = empty_frame();
        v["data"]["values"][1] = json!([5]);
        v["data"]["values"][2] = json!([5]);
        assert!(!flame_text(&v.to_string()).unwrap().is_empty_profile());
        for (col, bad) in [(0, json!(["bad"])), (2, json!(["bad"])), (1, json!([]))] {
            let mut v = empty_frame();
            v["data"]["values"][col] = bad;
            assert!(flame_text(&v.to_string()).is_err(), "column {col}");
        }
    }

    #[test]
    fn the_other_frame_kinds_refuse_malformed_parts() {
        let series_of = |t: &str| series(&frame(&json(t)?)?);
        each_break(
            series_frame(),
            series_of,
            &[
                ("another name", &|v| v["schema"]["name"] = json!("heatmap")),
                ("a third field", &|v| {
                    v["schema"]["fields"]
                        .as_array_mut()
                        .unwrap()
                        .push(json!({"name": "x", "type": "number"}));
                    v["data"]["values"]
                        .as_array_mut()
                        .unwrap()
                        .push(json!([1, 2]));
                }),
                ("time not a time", &|v| {
                    v["schema"]["fields"][0]["type"] = json!("number")
                }),
                ("a time a string", &|v| {
                    v["data"]["values"][0][0] = json!("1")
                }),
                ("a value a string", &|v| {
                    v["data"]["values"][1][0] = json!("0.5")
                }),
            ],
        );
        let heatmap_of = |t: &str| heatmap(&frame(&json(t)?)?);
        each_break(
            heatmap_frame(),
            heatmap_of,
            &[
                ("another name", &|v| v["schema"]["name"] = json!("series")),
                ("xMax a string", &|v| v["data"]["values"][0][0] = json!("1")),
                ("yMin a string", &|v| v["data"]["values"][1][0] = json!("0")),
                ("yMax null", &|v| v["data"]["values"][2][0] = Value::Null),
                ("yMin not below yMax", &|v| {
                    v["data"]["values"][1][0] = json!(2.0)
                }),
                ("a negative count", &|v| {
                    v["data"]["values"][3][0] = json!(-1)
                }),
                ("count a float", &|v| v["data"]["values"][3][0] = json!(1.5)),
                ("count missing", &|v| {
                    v["schema"]["fields"].as_array_mut().unwrap().pop();
                    v["data"]["values"].as_array_mut().unwrap().pop();
                }),
            ],
        );
        let exemplars_of = |t: &str| exemplars(&frame(&json(t)?)?);
        each_break(
            exemplar_frame(),
            exemplars_of,
            &[
                ("another name", &|v| v["schema"]["name"] = json!("series")),
                ("a Time a string", &|v| {
                    v["data"]["values"][0][0] = json!("1")
                }),
                ("a Value a string", &|v| {
                    v["data"]["values"][1][1] = json!("0.5")
                }),
                ("an Id a number", &|v| v["data"]["values"][2][0] = json!(1)),
                ("an Id empty", &|v| v["data"]["values"][2][0] = json!("")),
                ("a label column a number field", &|v| {
                    v["schema"]["fields"]
                        .as_array_mut()
                        .unwrap()
                        .push(json!({"name": "env", "type": "number"}));
                    v["data"]["values"]
                        .as_array_mut()
                        .unwrap()
                        .push(json!([1, 2]));
                }),
                ("a label value a number", &|v| {
                    v["schema"]["fields"]
                        .as_array_mut()
                        .unwrap()
                        .push(json!({"name": "env", "type": "string"}));
                    v["data"]["values"]
                        .as_array_mut()
                        .unwrap()
                        .push(json!(["prod", 2]));
                }),
                ("Id not a string field", &|v| {
                    v["schema"]["fields"][2]["type"] = json!("number")
                }),
            ],
        );
        let kind_of = |t: &str| kind(&frame(&json(t)?)?);
        each_break(
            series_frame(),
            kind_of,
            &[("an unknown frame name", &|v| {
                v["schema"]["name"] = json!("table")
            })],
        );
        let only_of = |t: &str| only_flame(&answer(t)?["A"]);
        each_break(
            answer_of(vec![flame_frame(5)]),
            only_of,
            &[
                ("no frame", &|v| v["results"]["A"]["frames"] = json!([])),
                ("two frames", &|v| {
                    v["results"]["A"]["frames"]
                        .as_array_mut()
                        .unwrap()
                        .push(flame_frame(5))
                }),
                ("a series instead", &|v| {
                    v["results"]["A"]["frames"] = json!([series_frame()])
                }),
            ],
        );
    }

    #[test]
    fn the_app_answers_refuse_every_malformed_part() {
        each_break(
            json!({"labelsSet": [{"labels": [{"name": "service_name", "value": "svc-a"}]}]}),
            series_answer,
            &[
                ("labelsSet missing", &|v| *v = json!({})),
                ("labels not an array", &|v| {
                    v["labelsSet"][0]["labels"] = json!({})
                }),
                ("a label value a number", &|v| {
                    v["labelsSet"][0]["labels"][0]["value"] = json!(1)
                }),
                ("a label without a name", &|v| {
                    v["labelsSet"][0]["labels"][0]
                        .as_object_mut()
                        .unwrap()
                        .remove("name");
                }),
            ],
        );
        each_break(
            json!({"series": [{"slots": [{"timestamp": "1", "yMin": [1.0, 2.0], "counts": [1, 0],
                "exemplars": [{"timestamp": "5", "spanId": "0102030405060708", "value": "10",
                               "traceId": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                               "labels": [{"name": "service_name", "value": "svc-otlp"}]}]}]}]}),
            heatmap_answer,
            &[
                ("no series", &|v| v["series"] = json!([])),
                ("slots missing", &|v| v["series"][0] = json!({})),
                ("timestamp missing", &|v| {
                    v["series"][0]["slots"][0]
                        .as_object_mut()
                        .unwrap()
                        .remove("timestamp");
                }),
                ("counts shorter than the buckets", &|v| {
                    v["series"][0]["slots"][0]["counts"] = json!([1])
                }),
                ("a count a string", &|v| {
                    v["series"][0]["slots"][0]["counts"][0] = json!("1")
                }),
                ("profiles but no exemplars", &|v| {
                    v["series"][0]["slots"][0]
                        .as_object_mut()
                        .unwrap()
                        .remove("exemplars");
                }),
                ("an exemplar value not an int64", &|v| {
                    v["series"][0]["slots"][0]["exemplars"][0]["value"] = json!("ten")
                }),
                ("an exemplar timestamp missing", &|v| {
                    v["series"][0]["slots"][0]["exemplars"][0]
                        .as_object_mut()
                        .unwrap()
                        .remove("timestamp");
                }),
                ("a span id of 15 digits", &|v| {
                    v["series"][0]["slots"][0]["exemplars"][0]["spanId"] = json!("010203040506070")
                }),
                ("a span id in capitals", &|v| {
                    v["series"][0]["slots"][0]["exemplars"][0]["spanId"] = json!("0A0B0C0D0E0F1011")
                }),
                ("a trace id too short", &|v| {
                    v["series"][0]["slots"][0]["exemplars"][0]["traceId"] = json!("bb")
                }),
                ("an exemplar label value a number", &|v| {
                    v["series"][0]["slots"][0]["exemplars"][0]["labels"][0]["value"] = json!(1)
                }),
                ("a bucket start a string", &|v| {
                    v["series"][0]["slots"][0]["yMin"][0] = json!("1")
                }),
                ("bucket starts not ascending", &|v| {
                    v["series"][0]["slots"][0]["yMin"] = json!([2.0, 1.0])
                }),
                ("a negative count", &|v| {
                    v["series"][0]["slots"][0]["counts"][1] = json!(-1)
                }),
                ("an exemplar value a number", &|v| {
                    v["series"][0]["slots"][0]["exemplars"][0]["value"] = json!(10)
                }),
                ("an exemplar without a span id", &|v| {
                    v["series"][0]["slots"][0]["exemplars"][0]
                        .as_object_mut()
                        .unwrap()
                        .remove("spanId");
                }),
            ],
        );
        each_break(
            json!({"leftTicks": 2, "rightTicks": 3, "flamebearer": {"numTicks": 5}}),
            diff_answer,
            &[
                ("leftTicks missing", &|v| {
                    v.as_object_mut().unwrap().remove("leftTicks");
                }),
                ("rightTicks a string", &|v| v["rightTicks"] = json!("3")),
                ("numTicks not the sum", &|v| {
                    v["flamebearer"]["numTicks"] = json!(6)
                }),
                ("flamebearer missing", &|v| {
                    v.as_object_mut().unwrap().remove("flamebearer");
                }),
            ],
        );
        each_break(
            json!({"flamebearerProfile": "{\"flamebearer\":{\"numTicks\":7}}"}),
            upload_answer,
            &[
                ("the profile an object, not a string", &|v| {
                    v["flamebearerProfile"] = json!({"flamebearer": {"numTicks": 7}})
                }),
                ("the profile not JSON", &|v| {
                    v["flamebearerProfile"] = json!("nope")
                }),
                ("numTicks missing", &|v| {
                    v["flamebearerProfile"] = json!("{\"flamebearer\":{}}")
                }),
                ("numTicks a float", &|v| {
                    v["flamebearerProfile"] = json!("{\"flamebearer\":{\"numTicks\":7.5}}")
                }),
            ],
        );
        each_break(
            json!({"settings": [{"name": "pluginSettings", "value": "{\"maxNodes\":12345}"}]}),
            plugin_settings,
            &[
                ("settings missing", &|v| *v = json!({})),
                ("no pluginSettings", &|v| {
                    v["settings"][0]["name"] = json!("other")
                }),
                ("two pluginSettings", &|v| {
                    let again = v["settings"][0].clone();
                    v["settings"].as_array_mut().unwrap().push(again);
                }),
                ("the value not JSON", &|v| {
                    v["settings"][0]["value"] = json!("{")
                }),
                ("the value an object", &|v| {
                    v["settings"][0]["value"] = json!({"maxNodes": 1})
                }),
            ],
        );
        each_break(
            json!({"code": "unimplemented"}),
            connect_code,
            &[
                ("code missing", &|v| *v = json!({"message": "x"})),
                ("code a number", &|v| v["code"] = json!(12)),
            ],
        );
        each_break(
            json!({"queries": [{"refId": "A"}]}),
            request_queries,
            &[
                ("queries missing", &|v| *v = json!({})),
                ("a query not an object", &|v| v["queries"][0] = json!("A")),
            ],
        );
        each_break(
            json!({"status": "OK"}),
            health,
            &[
                ("status ERROR", &|v| v["status"] = json!("ERROR")),
                ("status missing", &|v| *v = json!({})),
            ],
        );
        each_break(
            json!({"batches": [{"scopeSpans": [{"spans": [{}, {}]}]}]}),
            trace_span_count,
            &[
                ("neither key", &|v| *v = json!({})),
                ("both keys", &|v| v["resourceSpans"] = json!([])),
                ("scopeSpans missing", &|v| v["batches"][0] = json!({})),
                ("spans not an array", &|v| {
                    v["batches"][0]["scopeSpans"][0]["spans"] = json!({})
                }),
            ],
        );
        assert_eq!(
            trace_span_count(r#"{"batches":[{"scopeSpans":[{"spans":[{},{}]}]}]}"#),
            Ok(2)
        );
        each_break(
            json!({"info": {"version": "3.2.0"}, "enabled": true}),
            plugin_info,
            &[
                ("info missing", &|v| {
                    v.as_object_mut().unwrap().remove("info");
                }),
                ("version a number", &|v| v["info"]["version"] = json!(3)),
                ("enabled a string", &|v| v["enabled"] = json!("true")),
                ("enabled missing", &|v| {
                    v.as_object_mut().unwrap().remove("enabled");
                }),
            ],
        );
        each_break(
            json!({"uid": "x"}),
            datasource_uid,
            &[
                ("uid missing", &|v| *v = json!({})),
                ("uid a number", &|v| v["uid"] = json!(1)),
            ],
        );
        each_break(
            json!({"queries": [{"refId": "A", "spanSelector": ["01"], "labelSelector": "{}",
                                "profileTypeId": "cpu"}]}),
            request_queries,
            &[
                ("refId missing", &|v| {
                    v["queries"][0].as_object_mut().unwrap().remove("refId");
                }),
                ("spanSelector a string", &|v| {
                    v["queries"][0]["spanSelector"] = json!("01")
                }),
                ("a span id a number", &|v| {
                    v["queries"][0]["spanSelector"][0] = json!(1)
                }),
                ("labelSelector a number", &|v| {
                    v["queries"][0]["labelSelector"] = json!(1)
                }),
                ("profileTypeId null", &|v| {
                    v["queries"][0]["profileTypeId"] = Value::Null
                }),
            ],
        );
        each_break(
            json!([{"name": "P1", "complete": true, "error": null, "records": [], "failed": [],
                    "absentFound": [], "visible": [], "invisible": []}]),
            pages,
            &[
                ("failed missing", &|v| {
                    v[0].as_object_mut().unwrap().remove("failed");
                }),
                ("records missing", &|v| {
                    v[0].as_object_mut().unwrap().remove("records");
                }),
                ("an unknown field", &|v| v[0]["extra"] = json!(1)),
                ("a record with an unknown field", &|v| {
                    v[0]["records"] = json!([{"status": 200,
                    "method": "GET", "path": "/api/x", "request": "", "body": "",
                    "bodyError": null, "failure": null, "x": 1}])
                }),
                ("a failure without a reason", &|v| {
                    v[0]["failed"] = json!([{"method": "POST",
                    "path": "/api/ds/query", "request": ""}])
                }),
            ],
        );
        each_break(
            json!(["a", "b"]),
            strings,
            &[
                ("an element a number", &|v| v[1] = json!(2)),
                ("not an array", &|v| *v = json!({"a": 1})),
            ],
        );
    }
}
