//! The tree binary (`format=tree`, `Content-Type: binary/octet-stream+tree`),
//! read as the reference's `DeserializeNoDict` and `Iterate` read it
//! (`pkg/og/storage/tree/serialize.go:17-71`, `pkg/og/convert/parser.go:15-35`).

use super::{LegacyError, StackSet};

/// Every node with a self value adds it to the stack its path spells.
pub fn parse_tree(_body: &[u8]) -> Result<StackSet, LegacyError> {
    Err(LegacyError::Parse("not built".to_string()))
}

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::*;

    #[test]
    fn r1_the_reference_tree_reads() {
        assert_eq!(
            stacks_of(parse_tree(TREE)),
            Ok(map(&[(&["a", "b"], 10_000_000), (&["a", "c"], 20_000_000)]))
        );
    }

    #[test]
    fn r2_the_limits_and_short_bodies_refuse() {
        let mut long = vec![0xffu8; 9];
        long.push(0x01);
        assert_eq!(
            parse_tree(&long).map(|_| ()),
            Err(LegacyError::Parse(
                "tree node name length 18446744073709551615 exceeds maximum 65535".to_string()
            ))
        );
        assert_eq!(
            parse_tree(b"\x00\x00\x81\x7d").map(|_| ()),
            Err(LegacyError::Parse(
                "tree node children count 16001 exceeds maximum 16000".to_string()
            ))
        );
        assert_eq!(
            parse_tree(b"\x00\x00\x01\x01a").map(|_| ()),
            Err(LegacyError::Parse("EOF".to_string()))
        );
    }
}
