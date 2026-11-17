//! The trie binary (`format=trie`, `Content-Type: binary/octet-stream+trie`),
//! read as the reference's `IterateRaw` reads it
//! (`pkg/og/structs/transporttrie/serialize.go:54-124`).

use super::{EOF, LegacyError, Reader, StackSet, parse_error};

/// A node with descendants still to read: how many, and how many key bytes
/// its name added.
struct Offset {
    desc_count: i64,
    suffix_len: usize,
}

/// Every node with a value adds it to the stack its key spells. Each node is
/// a name length, the name appended to the key, a value and a descendant
/// count; end of input at a node's start ends the trie. A name length of 0
/// keeps the previous node's name length for the truncation, as the
/// reference does.
pub fn parse_trie(body: &[u8]) -> Result<StackSet, LegacyError> {
    let mut r = Reader::new(body);
    let mut set = StackSet::default();
    let mut key: Vec<u8> = Vec::with_capacity(256);
    let mut offsets: Vec<Offset> = Vec::new();
    let mut copied: usize = 0;
    loop {
        let name_len = match r.uvarint() {
            Ok(n) => n,
            Err(EOF) => return Ok(set),
            Err(e) => return Err(parse_error(e)),
        };
        if name_len != 0 {
            // `io.LimitReader` with a negative `int64` reads nothing.
            let n = i64::try_from(name_len).map_or(0, |n| n as usize);
            let name = r.take_up_to(n);
            key.extend_from_slice(name);
            copied = name.len();
        }
        let value = r.uvarint().map_err(parse_error)?;
        let desc_count = r.uvarint().map_err(parse_error)?;
        if value > 0 {
            set.add_text(&key, value);
        }
        if desc_count != 0 {
            offsets.push(Offset {
                desc_count: desc_count as i64,
                suffix_len: copied,
            });
            continue;
        }
        // A leaf: cut its name, then every ancestor with no descendant left,
        // short of the root.
        key.truncate(key.len().saturating_sub(copied));
        let Some(mut i) = offsets.len().checked_sub(1) else {
            continue;
        };
        offsets[i].desc_count = offsets[i].desc_count.wrapping_sub(1);
        while i > 0 {
            if offsets[i].desc_count != 0 {
                break;
            }
            key.truncate(key.len().saturating_sub(offsets[i].suffix_len));
            offsets.truncate(i);
            if let Some(p) = offsets.len().checked_sub(1)
                && p > 0
            {
                offsets[p].desc_count = offsets[p].desc_count.wrapping_sub(1);
            }
            i -= 1;
        }
    }
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
