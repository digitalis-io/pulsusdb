//! The trie binary (`format=trie`, `Content-Type: binary/octet-stream+trie`),
//! read as the reference's `IterateRaw` reads it
//! (`pkg/og/structs/transporttrie/serialize.go:54-124`).

use super::{LegacyError, StackSet};

/// Every node with a value adds it to the stack its key spells.
pub fn parse_trie(_body: &[u8]) -> Result<StackSet, LegacyError> {
    Err(LegacyError::Parse("not built".to_string()))
}

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::*;

    #[test]
    fn t1_the_reference_trie_reads() {
        assert_eq!(
            stacks_of(parse_trie(TRIE)),
            Ok(map(&[(&["abc"], 10_000_000), (&["abd"], 20_000_000)]))
        );
    }

    #[test]
    fn t2_keys_split_on_semicolons_and_short_bodies_refuse() {
        assert_eq!(
            stacks_of(parse_trie(
                b"\x00\x00\x01\x06foo;ba\x00\x02\x01r\x0a\x00\x01z\x14\x00"
            )),
            Ok(map(&[
                (&["foo", "bar"], 100_000_000),
                (&["foo", "baz"], 200_000_000)
            ]))
        );
        assert_eq!(
            parse_trie(b"\x00\x00\x01\x02ab").map(|_| ()),
            Err(LegacyError::Parse("EOF".to_string()))
        );
        assert_eq!(stacks_of(parse_trie(b"")), Ok(map(&[])));
    }
}
