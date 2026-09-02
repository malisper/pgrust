//! Versioned key encoding: `<row key>/<u64::MAX - seq>`.
//!
//! The sequence number is inverted so a row's versions sort newest-first, which
//! makes "read as of snapshot S" a single seek to `<row>/<MAX-S>` rather than a
//! walk back through history.
//!
//! Only run files carry versioned keys. Commit objects keep bare row keys and
//! inherit their sequence number from the commit header, because a transaction
//! does not know its number until it has won the PUT that assigns it.

/// Read the present. A smaller value reads the past.
pub const LATEST: u64 = u64::MAX;

const SUFFIX_LEN: usize = 1 + 16;

pub fn versioned(row_key: &[u8], seq: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(row_key.len() + SUFFIX_LEN);
    out.extend_from_slice(row_key);
    out.push(b'/');
    out.extend_from_slice(format!("{:016x}", u64::MAX - seq).as_bytes());
    out
}

/// Seek here to read `row_key` as of `snapshot`; the first entry at or after
/// it is the version that was live then.
pub fn seek_at(row_key: &[u8], snapshot: u64) -> Vec<u8> {
    versioned(row_key, snapshot)
}

pub fn row_of(versioned_key: &[u8]) -> Option<&[u8]> {
    if versioned_key.len() < SUFFIX_LEN {
        return None;
    }
    let cut = versioned_key.len() - SUFFIX_LEN;
    if versioned_key[cut] != b'/' {
        return None;
    }
    Some(&versioned_key[..cut])
}

pub fn seq_of(versioned_key: &[u8]) -> Option<u64> {
    if versioned_key.len() < SUFFIX_LEN {
        return None;
    }
    let tail = &versioned_key[versioned_key.len() - 16..];
    let inv = u64::from_str_radix(std::str::from_utf8(tail).ok()?, 16).ok()?;
    Some(u64::MAX - inv)
}

pub fn belongs_to(versioned_key: &[u8], row_key: &[u8]) -> bool {
    row_of(versioned_key) == Some(row_key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        let k = versioned(b"0001/0002", 7);
        assert_eq!(row_of(&k), Some(&b"0001/0002"[..]));
        assert_eq!(seq_of(&k), Some(7));
        assert!(belongs_to(&k, b"0001/0002"));
        assert!(!belongs_to(&k, b"0001/0003"));
    }

    #[test]
    fn newer_versions_sort_first() {
        let old = versioned(b"r", 1);
        let mid = versioned(b"r", 5);
        let new = versioned(b"r", 9);
        assert!(new < mid && mid < old, "higher seq must sort earlier");
    }

    #[test]
    fn a_seek_lands_on_the_newest_version_at_or_below_the_snapshot() {
        let mut keys = vec![versioned(b"r", 1), versioned(b"r", 5), versioned(b"r", 9)];
        keys.sort();
        let probe = seek_at(b"r", 5);
        let found = keys.iter().find(|k| **k >= probe).unwrap();
        assert_eq!(seq_of(found), Some(5));

        let probe = seek_at(b"r", 0);
        assert!(keys.iter().find(|k| **k >= probe).is_none_or(|k| seq_of(k) == Some(0)));

        let probe = seek_at(b"r", LATEST);
        assert_eq!(seq_of(keys.iter().find(|k| **k >= probe).unwrap()), Some(9));
    }

    #[test]
    fn an_unversioned_key_is_rejected_rather_than_misparsed() {
        assert_eq!(row_of(b"short"), None);
        assert_eq!(row_of(b"0001/0002"), None, "no suffix separator at the cut");
    }
}
