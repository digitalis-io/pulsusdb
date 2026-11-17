//! The tree binary (`format=tree`, `Content-Type: binary/octet-stream+tree`),
//! read as the reference's `DeserializeNoDict` and `Iterate` read it
//! (`pkg/og/storage/tree/serialize.go:17-71`, `pkg/og/convert/parser.go:15-35`).

use super::{LegacyError, Reader, StackSet, parse_error};
use crate::protocols::profile_model::{MAX_PROFILE_SAMPLES, MAX_SYMBOL_BYTES};

/// Every node with a self value adds it to the stack its path spells. Each
/// node is a name length, the name, a self value and a child count, its
/// children following it depth first. A node's key is its parent's, `;` and
/// its name, the root's `;` and its name; a key longer than two bytes and a
/// self value other than 0 add the key past its first two bytes. Two
/// siblings of one name sum, where the reference keeps the later one's value
/// (issue #818).
pub fn parse_tree(body: &[u8]) -> Result<StackSet, LegacyError> {
    let mut r = Reader::new(body);
    let mut set = StackSet::default();
    let mut key: Vec<u8> = Vec::new();
    // Each node still owed children: its key's length and how many.
    let mut parents: Vec<(usize, u64)> = vec![(0, 1)];
    while let Some(top) = parents.last_mut() {
        let parent_len = top.0;
        top.1 -= 1;
        if top.1 == 0 {
            parents.pop();
        }
        key.truncate(parent_len);
        let name_len = r.uvarint().map_err(parse_error)?;
        if name_len > MAX_SYMBOL_BYTES as u64 {
            return Err(LegacyError::Parse(format!(
                "tree node name length {name_len} exceeds maximum {MAX_SYMBOL_BYTES}"
            )));
        }
        let name = r.take_exact(name_len as usize).map_err(parse_error)?;
        key.push(b';');
        key.extend_from_slice(name);
        let self_value = r.uvarint().map_err(parse_error)?;
        let children = r.uvarint().map_err(parse_error)?;
        if children > MAX_PROFILE_SAMPLES as u64 {
            return Err(LegacyError::Parse(format!(
                "tree node children count {children} exceeds maximum {MAX_PROFILE_SAMPLES}"
            )));
        }
        if key.len() > 2 && self_value != 0 {
            set.add_text(&key[2..], self_value);
        }
        if children > 0 {
            parents.push((key.len(), children));
        }
    }
    Ok(set)
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
