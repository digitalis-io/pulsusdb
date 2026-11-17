//! Folded stacks, `<frame>;<frame> <count>` per line (`groups`), and
//! individual lines, one stack per line counted (`lines`)
//! (`pkg/og/convert/parser.go:42-100`).

use super::{LegacyError, StackSet};

/// `groups`: each line's stack and count, summed per stack.
pub fn parse_groups(_body: &[u8]) -> Result<StackSet, LegacyError> {
    Err(LegacyError::Parse("not built".to_string()))
}

/// `lines`: each line is one stack, counted.
pub fn parse_lines(_body: &[u8]) -> Result<StackSet, LegacyError> {
    Err(LegacyError::Parse("not built".to_string()))
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
