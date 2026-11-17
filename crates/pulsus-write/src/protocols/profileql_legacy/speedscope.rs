//! Speedscope JSON (`format=speedscope`), read as the reference reads it
//! (`pkg/og/convert/speedscope/parser.go`, `units.go`, `json.go`), with the
//! three value defects of issue #818's Q3 corrected.

use std::collections::HashMap;

use serde::Deserialize;

use super::{LegacyError, StackSet, put_input};
use crate::ingest::profiles::ProfileInput;
use crate::protocols::profile_push::{Labels, LegacyQuery};

const SCHEMA: &str = "https://www.speedscope.app/file-format-schema.json";

/// The file, as far as the reference reads it (`json.go`); other fields are
/// ignored.
#[derive(Deserialize, Default)]
#[serde(default)]
struct File {
    #[serde(rename = "$schema")]
    schema: String,
    shared: Shared,
    profiles: Vec<SpeedscopeProfile>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct Shared {
    frames: Vec<Frame>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct Frame {
    name: String,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct SpeedscopeProfile {
    #[serde(rename = "type")]
    kind: String,
    name: String,
    unit: String,
    #[serde(rename = "startValue")]
    start_value: f64,
    events: Vec<Event>,
    samples: Vec<Vec<f64>>,
    weights: Vec<f64>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct Event {
    #[serde(rename = "type")]
    kind: String,
    at: f64,
    frame: f64,
}

fn parse_error(message: String) -> LegacyError {
    LegacyError::Parse(message)
}

fn unknown_unit(unit: &str) -> LegacyError {
    parse_error(format!("invalid profile unit: unknown unit: {unit}"))
}

/// The sample rate a unit takes when the push gave the default 100
/// (`units.go:25-46`); `nanoseconds` is 10^9, where the reference's `uint32`
/// arithmetic overflows (issue #818, Q3).
fn default_sample_rate(unit: &str) -> Result<u32, LegacyError> {
    match unit {
        "nanoseconds" => Ok(1_000_000_000),
        "microseconds" => Ok(100_000_000),
        "milliseconds" => Ok(100_000),
        "seconds" => Ok(100),
        "none" => Ok(10_000),
        "bytes" => Ok(0),
        _ => Err(unknown_unit(unit)),
    }
}

/// What a weight is multiplied by before it is stored (`units.go:48-56`);
/// `nanoseconds` is 1, its rate carrying the whole scale (issue #818, Q3).
fn precision(unit: &str) -> Result<f64, LegacyError> {
    match unit {
        "microseconds" | "milliseconds" | "seconds" | "none" => Ok(100.0),
        "nanoseconds" | "bytes" => Ok(1.0),
        _ => Err(unknown_unit(unit)),
    }
}

/// A weight or a time gap as a stored value: multiplied before it is
/// truncated (issue #818, Q3).
fn value(w: f64, precision: f64) -> u64 {
    (w * precision) as u64
}

/// Profiles equal in name, type, unit and start concatenate their samples,
/// events and weights; the result is ordered by start (`parser.go:71-107`).
fn merge(profiles: Vec<SpeedscopeProfile>) -> Vec<SpeedscopeProfile> {
    let mut merged: Vec<SpeedscopeProfile> = Vec::with_capacity(profiles.len());
    let mut index: HashMap<(String, String, String, u64), usize> = HashMap::new();
    for p in profiles {
        // `+ 0.0` makes `-0` and `0` one key, as Go's float comparison does.
        let key = (
            p.name.clone(),
            p.kind.clone(),
            p.unit.clone(),
            (p.start_value + 0.0).to_bits(),
        );
        match index.get(&key) {
            Some(&i) => {
                let m = &mut merged[i];
                m.samples.extend(p.samples);
                m.events.extend(p.events);
                m.weights.extend(p.weights);
            }
            None => {
                index.insert(key, merged.len());
                merged.push(p);
            }
        }
    }
    merged.sort_by(|a, b| {
        a.start_value
            .partial_cmp(&b.start_value)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    merged
}

/// `parseEvented` (`parser.go:154-202`): a close adds the time since the
/// last event to the open stack, an open adds it to the stack before it.
fn parse_evented(
    set: &mut StackSet,
    p: &SpeedscopeProfile,
    frames: &[Frame],
) -> Result<(), LegacyError> {
    let precision = precision(&p.unit)?;
    let mut last = p.start_value;
    let mut open: Vec<usize> = Vec::new();
    let mut names: Vec<&str> = Vec::new();
    for ev in &p.events {
        if ev.at < last {
            return Err(parse_error(format!(
                "Events out of order, {:.6} < {:.6}",
                ev.at, last
            )));
        }
        let fid = ev.frame as i64;
        if fid < 0 || fid >= frames.len() as i64 {
            return Err(parse_error(format!("Invalid frame {fid}")));
        }
        let fid = fid as usize;
        match ev.kind.as_str() {
            "C" => {
                let Some(&top) = open.last() else {
                    return Err(parse_error(format!("No stack to close at {:.6}", ev.at)));
                };
                if top != fid {
                    return Err(parse_error(format!("Closing non-open frame {fid}")));
                }
                set.add_frames(&names, value(ev.at - last, precision));
                open.pop();
                names.pop();
            }
            "O" => {
                if !names.is_empty() {
                    set.add_frames(&names, value(ev.at - last, precision));
                }
                open.push(fid);
                names.push(&frames[fid].name);
            }
            other => return Err(parse_error(format!("Unknown event type {other}"))),
        }
        last = ev.at;
    }
    Ok(())
}

/// `parseSampled` (`parser.go:204-232`). A frame index equal to the frame
/// count is refused, where the reference panics.
fn parse_sampled(
    set: &mut StackSet,
    p: &SpeedscopeProfile,
    frames: &[Frame],
) -> Result<(), LegacyError> {
    if p.samples.len() != p.weights.len() {
        return Err(parse_error(format!(
            "Unequal lengths of samples and weights: {} != {}",
            p.samples.len(),
            p.weights.len()
        )));
    }
    let precision = precision(&p.unit)?;
    let mut stack: Vec<&str> = Vec::new();
    for (sample, &w) in p.samples.iter().zip(&p.weights) {
        if w < 0.0 {
            return Err(parse_error(format!("Negative weight {w:.6}")));
        }
        stack.clear();
        for &frame in sample {
            let fid = frame as i64;
            if fid < 0 || fid >= frames.len() as i64 {
                return Err(parse_error(format!("Invalid frame {fid}")));
            }
            stack.push(&frames[fid as usize].name);
        }
        set.add_frames(&stack, value(w, precision));
    }
    Ok(())
}

/// Sets `key` to `value`, or removes it when `value` is empty, as the
/// reference's `LabelSet.Add` does.
fn set_label(labels: &mut Labels, key: &str, value: &str) {
    labels.retain(|(k, _)| k != key);
    if !value.is_empty() {
        labels.push((key.to_string(), value.to_string()));
    }
}

/// Every profile of the file, merged and parsed before any is built
/// (`parser.go:38-152`). A file of several profiles names each
/// `<service>.<unit>`; each carries its name as `profile_name`. One push,
/// stored whole or not at all.
pub fn parse_speedscope(
    body: &[u8],
    query: &LegacyQuery,
) -> Result<Vec<ProfileInput>, LegacyError> {
    let file: File = serde_json::from_slice(body).map_err(|e| parse_error(e.to_string()))?;
    if file.schema != SCHEMA {
        return Err(parse_error(format!("Unknown schema: {}", file.schema)));
    }
    let frames = &file.shared.frames;
    let profiles = merge(file.profiles);
    let multi = profiles.len() > 1;
    let service = query
        .name
        .iter()
        .find(|(k, _)| k == "__name__")
        .map_or("", |(_, v)| v.as_str());

    let mut parsed = Vec::with_capacity(profiles.len());
    for p in &profiles {
        let mut labels = query.name.clone();
        if multi {
            set_label(&mut labels, "__name__", &format!("{service}.{}", p.unit));
        }
        set_label(&mut labels, "profile_name", &p.name);
        let sample_rate = if query.sample_rate == 100 {
            default_sample_rate(&p.unit)?
        } else {
            query.sample_rate
        };
        let mut set = StackSet::default();
        match p.kind.as_str() {
            "evented" => parse_evented(&mut set, p, frames)?,
            "sampled" => parse_sampled(&mut set, p, frames)?,
            other => {
                return Err(parse_error(format!("Profile type {other} not supported")));
            }
        }
        parsed.push((set, labels, sample_rate));
    }

    let mut inputs = Vec::with_capacity(parsed.len());
    for (set, labels, sample_rate) in parsed {
        let q = LegacyQuery {
            sample_rate,
            ..query.clone()
        };
        inputs.push(put_input(set, &q, &labels)?);
    }
    Ok(inputs)
}

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::*;

    fn parse(body: &str, name: &str) -> Result<Vec<ProfileInput>, LegacyError> {
        parse_speedscope(body.as_bytes(), &query(name, 100))
    }

    /// Each input's `__name__`, sample type, `profile_name` and stacks.
    type Summary = (Option<String>, String, Option<String>, Stacks);

    fn summary(
        inputs: Result<Vec<ProfileInput>, LegacyError>,
    ) -> Result<Vec<Summary>, LegacyError> {
        inputs.map(|inputs| {
            inputs
                .iter()
                .map(|i| {
                    (
                        label(i, "__name__").map(str::to_string),
                        sample_type(i),
                        label(i, "profile_name").map(str::to_string),
                        stacks(i),
                    )
                })
                .collect()
        })
    }

    fn one_profile(profile: &str) -> String {
        format!(
            r#"{{"$schema":"https://www.speedscope.app/file-format-schema.json",
                "shared":{{"frames":[{{"name":"a"}},{{"name":"b"}}]}},
                "profiles":[{profile}]}}"#
        )
    }

    fn stacks_one(profile: &str) -> Result<Stacks, LegacyError> {
        parse(&one_profile(profile), "app").map(|inputs| {
            assert_eq!(inputs.len(), 1);
            stacks(&inputs[0])
        })
    }

    #[test]
    fn s1_an_evented_profile_reads() {
        assert_eq!(
            summary(parse(SIMPLE, "foo")),
            Ok(vec![(
                Some("process_cpu".to_string()),
                "cpu/nanoseconds".to_string(),
                Some("simple.txt".to_string()),
                map(&[
                    (&["a", "b"], 50_000_000),
                    (&["a", "b", "c"], 50_000_000),
                    (&["a", "b", "d"], 40_000_000),
                ])
            )])
        );
    }

    #[test]
    fn s2_two_sampled_profiles_are_two_inputs() {
        let inputs = parse(&two_sampled(), "foo{x=y}");
        let want = |profile: &str| {
            (
                Some("wall".to_string()),
                "wall/nanoseconds".to_string(),
                Some(profile.to_string()),
                map(&[
                    (&["a", "b"], 5_000_000_000),
                    (&["a", "b", "c"], 5_000_000_000),
                    (&["a", "b", "d"], 4_000_000_000),
                ]),
            )
        };
        let labels: Result<Vec<_>, _> = inputs.as_ref().map(|inputs| {
            inputs
                .iter()
                .map(|i| {
                    (
                        label(i, "service_name").map(str::to_string),
                        label(i, "x").map(str::to_string),
                    )
                })
                .collect::<Vec<_>>()
        });
        let foo = (Some("foo".to_string()), Some("y".to_string()));
        assert_eq!(labels.map_err(|e| e.clone()), Ok(vec![foo.clone(), foo]));
        assert_eq!(summary(inputs), Ok(vec![want("one"), want("two")]));
    }

    /// The reference's `duplicates.speedscope.json`: one profile listed three
    /// times.
    #[test]
    fn s3_equal_profiles_merge() {
        let body = speedscope_file(&[
            sampled_profile("one", 14),
            sampled_profile("one", 14),
            sampled_profile("one", 52),
        ]);
        assert_eq!(
            summary(parse(&body, "foo{x=y}")),
            Ok(vec![(
                Some("process_cpu".to_string()),
                "cpu/nanoseconds".to_string(),
                Some("one".to_string()),
                map(&[
                    (&["a", "b"], 15_000_000_000),
                    (&["a", "b", "c"], 15_000_000_000),
                    (&["a", "b", "d"], 12_000_000_000),
                ])
            )])
        );
    }

    #[test]
    fn s4_each_refusal_carries_the_reference_text() {
        let schema = r#"{"$schema":"x","profiles":[]}"#.to_string();
        let evented = |events: &str| {
            one_profile(&format!(
                r#"{{"type":"evented","name":"p","unit":"none","startValue":0,"events":[{events}]}}"#
            ))
        };
        let sampled = |samples: &str, weights: &str| {
            one_profile(&format!(
                r#"{{"type":"sampled","name":"p","unit":"none","samples":{samples},"weights":{weights}}}"#
            ))
        };
        let cases = [
            (schema, "Unknown schema: x"),
            (
                one_profile(r#"{"type":"sampled","name":"p","unit":"furlong"}"#),
                "invalid profile unit: unknown unit: furlong",
            ),
            (
                one_profile(r#"{"type":"x","name":"p","unit":"none"}"#),
                "Profile type x not supported",
            ),
            (
                evented(r#"{"type":"O","frame":0,"at":2},{"type":"O","frame":1,"at":1}"#),
                "Events out of order, 1.000000 < 2.000000",
            ),
            (
                evented(r#"{"type":"O","frame":0,"at":0},{"type":"C","frame":1,"at":1}"#),
                "Closing non-open frame 1",
            ),
            (
                evented(r#"{"type":"C","frame":0,"at":1}"#),
                "No stack to close at 1.000000",
            ),
            (
                evented(r#"{"type":"O","frame":2,"at":1}"#),
                "Invalid frame 2",
            ),
            (
                evented(r#"{"type":"X","frame":0,"at":1}"#),
                "Unknown event type X",
            ),
            (
                sampled("[[0]]", "[]"),
                "Unequal lengths of samples and weights: 1 != 0",
            ),
            (sampled("[[0]]", "[-1]"), "Negative weight -1.000000"),
            (sampled("[[0, 2]]", "[1]"), "Invalid frame 2"),
        ];
        for (body, message) in cases {
            assert_eq!(
                parse(&body, "app").map(|i| i.len()),
                Err(LegacyError::Parse(message.to_string())),
                "{body}"
            );
        }
    }

    /// Q3: values are computed before truncation, an open gap takes the
    /// precision multiplier, and `nanoseconds` stores its weights.
    #[test]
    fn s5_speedscope_values_are_computed_correctly() {
        assert_eq!(
            stacks_one(
                r#"{"type":"sampled","name":"p","unit":"seconds","samples":[[0]],"weights":[0.5]}"#
            ),
            Ok(map(&[(&["a"], 500_000_000)])),
            "a"
        );
        assert_eq!(
            stacks_one(
                r#"{"type":"evented","name":"p","unit":"none","startValue":0,"events":[
                    {"type":"O","frame":0,"at":0},{"type":"O","frame":1,"at":3},
                    {"type":"C","frame":1,"at":5},{"type":"C","frame":0,"at":6}]}"#
            ),
            Ok(map(&[(&["a"], 40_000_000), (&["a", "b"], 20_000_000)])),
            "b"
        );
        assert_eq!(
            stacks_one(
                r#"{"type":"sampled","name":"p","unit":"nanoseconds","samples":[[0]],"weights":[1000]}"#
            ),
            Ok(map(&[(&["a"], 1000)])),
            "c"
        );
    }
}
