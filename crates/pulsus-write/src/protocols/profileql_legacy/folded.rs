//! Folded stacks, `<frame>;<frame> <count>` per line (`groups`), and
//! individual lines, one stack per line counted (`lines`)
//! (`pkg/og/convert/parser.go:42-100`).

use std::collections::HashMap;

use super::{LegacyError, StackSet};

/// `bufio.MaxScanTokenSize`: the reference's line scanner refuses a line
/// this long or longer before its `\n`.
const MAX_LINE_BYTES: usize = 65_536;

/// The body's lines as `bufio.ScanLines` gives them, each with its `\r`
/// still on: split on `\n`, the empty piece after a final `\n` skipped.
fn raw_lines(body: &[u8]) -> impl Iterator<Item = &[u8]> {
    let body = body.strip_suffix(b"\n").unwrap_or(body);
    let empty = body.is_empty();
    body.split(|&b| b == b'\n').filter(move |_| !empty)
}

/// One trailing `\r` removed (`bufio.dropCR`).
fn drop_cr(line: &[u8]) -> &[u8] {
    line.strip_suffix(b"\r").unwrap_or(line)
}

/// Go's `strconv.Atoi` of a count, refused with its text.
fn atoi(count: &[u8]) -> Result<i64, LegacyError> {
    use std::num::IntErrorKind;
    let text = String::from_utf8_lossy(count);
    text.parse::<i64>().map_err(|e| {
        let why = match e.kind() {
            IntErrorKind::PosOverflow | IntErrorKind::NegOverflow => "value out of range",
            _ => "invalid syntax",
        };
        let quoted = text.replace('\\', "\\\\").replace('"', "\\\"");
        LegacyError::Parse(format!("strconv.Atoi: parsing \"{quoted}\": {why}"))
    })
}

/// `groups`: each line's stack, before its last space, and count, after it,
/// summed per stack (`parser.go:42-67`). A line without a space is skipped.
/// Every line is read, however long (issue #818, Q1).
pub fn parse_groups(body: &[u8]) -> Result<StackSet, LegacyError> {
    let mut counts: HashMap<&[u8], u64> = HashMap::new();
    for line in raw_lines(body).map(drop_cr) {
        let Some(space) = line.iter().rposition(|&b| b == b' ') else {
            continue;
        };
        let n = atoi(&line[space + 1..])?;
        let sum = counts.entry(&line[..space]).or_insert(0);
        *sum = sum.wrapping_add(n as u64);
    }
    let mut set = StackSet::default();
    for (text, v) in counts {
        set.add_text(text, v);
    }
    Ok(set)
}

/// `lines`: each line is one stack, counted; an empty line is skipped, and
/// a line of 64 KiB or more refuses the push (`parser.go:73-100`).
pub fn parse_lines(body: &[u8]) -> Result<StackSet, LegacyError> {
    let mut counts: HashMap<&[u8], u64> = HashMap::new();
    for raw in raw_lines(body) {
        if raw.len() >= MAX_LINE_BYTES {
            return Err(LegacyError::Parse(
                "bufio.Scanner: token too long".to_string(),
            ));
        }
        *counts.entry(drop_cr(raw)).or_insert(0) += 1;
    }
    let mut set = StackSet::default();
    for (text, v) in counts {
        if !text.is_empty() {
            set.add_text(text, v);
        }
    }
    Ok(set)
}

#[cfg(test)]
mod tests {
    use super::super::put_input;
    use super::super::test_support::*;
    use super::*;
    use crate::protocols::profile_model::SPY_LABEL;

    #[test]
    fn f1_folded_stacks_become_one_cpu_profile() {
        let q = query("app", 100);
        let got = parse_groups(b"a;b 3\na;c 2")
            .and_then(|s| put_input(s, &q, &q.name))
            .map(|i| (sample_type(&i), period(&i), stacks(&i), i.labels));
        let labels = [
            ("__name__", "process_cpu"),
            ("__delta__", "false"),
            (SPY_LABEL, "unknown"),
            ("service_name", "app"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect::<Vec<_>>();
        assert_eq!(
            got,
            Ok((
                "cpu/nanoseconds".to_string(),
                (10_000_000, Some("cpu/nanoseconds".to_string())),
                map(&[(&["a", "b"], 30_000_000), (&["a", "c"], 20_000_000)]),
                labels
            ))
        );
    }

    #[test]
    fn f2_folded_lines_are_read_as_the_reference_reads_them() {
        assert_eq!(
            stacks_of(parse_groups(b"a;b 3\na;b; 4\nnospace\n\nx;y 0\r\n")),
            Ok(map(&[(&["a", "b"], 70_000_000)]))
        );
    }

    #[test]
    fn f3_a_count_that_does_not_read_refuses_the_push() {
        assert_eq!(
            parse_groups(b"a;b x").map(|_| ()),
            Err(LegacyError::Parse(
                "strconv.Atoi: parsing \"x\": invalid syntax".to_string()
            ))
        );
        assert_eq!(
            parse_groups(b"a 99999999999999999999").map(|_| ()),
            Err(LegacyError::Parse(
                "strconv.Atoi: parsing \"99999999999999999999\": value out of range".to_string()
            ))
        );
    }

    #[test]
    fn f4_individual_lines_are_counted() {
        assert_eq!(
            stacks_of(parse_lines(b"a;b\na;b\na;c\n\n")),
            Ok(map(&[(&["a", "b"], 20_000_000), (&["a", "c"], 10_000_000)]))
        );
    }

    #[test]
    fn f5_a_line_of_64_kib_refuses_a_lines_push() {
        let mut long = vec![b'a'; 65_536];
        long.push(b'\n');
        assert_eq!(
            parse_lines(&long).map(|_| ()),
            Err(LegacyError::Parse(
                "bufio.Scanner: token too long".to_string()
            ))
        );
        let mut short = vec![b'a'; 65_535];
        short.push(b'\n');
        assert_eq!(parse_lines(&short).map(|_| ()), Ok(()));
    }

    /// Q1: the reference stops at a line of 64 KiB and drops the rest.
    #[test]
    fn f6_a_folded_line_of_any_length_is_read() {
        let body = format!("{}z 1\nb 1\n", "a;".repeat(33_000));
        let got = stacks_of(parse_groups(body.as_bytes()));
        assert_eq!(got.as_ref().map(|s| s.len()), Ok(2));
        assert_eq!(
            got.map(|s| s.get(&vec!["b".to_string()]).copied()),
            Ok(Some(10_000_000))
        );
    }
}
