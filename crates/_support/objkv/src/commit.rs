//! The commit object: `commit/<seq>`, one batch of writes, written once with
//! put-if-absent. Winning that PUT is what makes the commit real, so the
//! changeset is carried **inline** rather than referenced — one round trip,
//! not two.

use std::io;

pub const MAGIC: u32 = 0x4f4b_4356; // "OKCV"
/// Format 2 carries the writer's lease epoch where format 1 carried a
/// `confirmed_through` watermark and a self-confirmed flag. Every object a
/// writer produces is the commit once it lands, so nothing vouches for
/// anything; the epoch is what lets a later open tell a stale writer's object
/// from the owner's (see `lease.rs`). Format 1 objects are refused: a bucket
/// written by an older objkv has to be migrated, not guessed at.
pub const VERSION: u8 = 2;
const HEADER_LEN: usize = 44;

/// A batch object: several commits that landed in one PUT. Group commit
/// writes these; a single commit still writes the plain form above, so an
/// older server reads a bucket that never grouped exactly as before.
pub const BATCH_MAGIC: u32 = 0x4f4b_4342; // "OKCB"
pub const BATCH_VERSION: u8 = 1;
/// magic, version, three reserved bytes, count.
const BATCH_HEADER_LEN: usize = 4 + 1 + 3 + 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    Put(Vec<u8>),
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub key: Vec<u8>,
    pub op: Op,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    pub seq: u64,
    /// The sorted run this commit is layered on top of; readers walk commits
    /// back to here before consulting run files.
    pub base_run_id: u64,
    /// Names the writing transaction. A discard marker repeats it so the
    /// marker can be matched to the object it judges; otherwise it is only
    /// for the log.
    pub xid: u32,
    /// The lease epoch of the writer that produced this object. An object
    /// whose epoch is below a later owner's takeover fence is a stale
    /// writer's and is never applied.
    pub epoch: u64,
    pub entries: Vec<Entry>,
}

pub fn key_for(seq: u64) -> String {
    format!("commit/{seq:016x}")
}

impl Commit {
    pub fn encode(&self) -> Vec<u8> {
        let mut payload = Vec::new();
        for e in &self.entries {
            put_u32(&mut payload, e.key.len() as u32);
            payload.extend_from_slice(&e.key);
            match &e.op {
                Op::Put(v) => {
                    payload.push(0);
                    put_u32(&mut payload, v.len() as u32);
                    payload.extend_from_slice(v);
                }
                Op::Delete => {
                    payload.push(1);
                    put_u32(&mut payload, 0);
                }
            }
        }

        let mut out = Vec::with_capacity(HEADER_LEN + payload.len() + 4);
        put_u32(&mut out, MAGIC);
        out.push(VERSION);
        out.extend_from_slice(&[0, 0, 0]); // flags, reserved
        put_u64(&mut out, self.seq);
        put_u64(&mut out, self.base_run_id);
        put_u32(&mut out, self.entries.len() as u32);
        put_u32(&mut out, payload.len() as u32);
        put_u32(&mut out, self.xid);
        put_u64(&mut out, self.epoch);
        debug_assert_eq!(out.len(), HEADER_LEN);
        out.extend_from_slice(&payload);
        let crc = crc32c::pg_comp_crc32c(0xffff_ffff, &out) ^ 0xffff_ffff;
        put_u32(&mut out, crc);
        out
    }

    /// `encode`, refusing anything the u32 length fields cannot describe. A
    /// commit past 4 GiB would land with a wrong payload length and make the
    /// bucket unopenable, so the writer checks before the PUT.
    pub fn encode_checked(&self) -> io::Result<Vec<u8>> {
        let too_big = |what: &str| {
            io::Error::other(format!("objkv: commit too large to encode: {what} exceeds 4 GiB"))
        };
        let mut payload: u64 = 0;
        for e in &self.entries {
            if e.key.len() > u32::MAX as usize {
                return Err(too_big("a key"));
            }
            let vlen = match &e.op {
                Op::Put(v) => v.len(),
                Op::Delete => 0,
            };
            if vlen > u32::MAX as usize {
                return Err(too_big("a value"));
            }
            payload += 4 + e.key.len() as u64 + 1 + 4 + vlen as u64;
        }
        if self.entries.len() > u32::MAX as usize || payload > u32::MAX as u64 {
            return Err(too_big("the payload"));
        }
        Ok(self.encode())
    }

    /// Never panics: length fields come off the network and may disagree with
    /// the buffer.
    pub fn decode(buf: &[u8]) -> io::Result<Commit> {
        fn bad(what: &str) -> io::Error {
            io::Error::other(format!("malformed commit object: {what}"))
        }
        fn take<'a>(b: &'a [u8], at: usize, n: usize) -> Option<&'a [u8]> {
            b.get(at..at.checked_add(n)?)
        }

        if buf.len() < HEADER_LEN + 4 {
            return Err(bad("shorter than a header"));
        }
        if get_u32(buf, 0) != MAGIC {
            return Err(bad("bad magic"));
        }
        if buf[4] != VERSION {
            return Err(bad(&format!(
                "unsupported version {} (this objkv reads format {VERSION} only; an older \
                 bucket must be migrated, not opened)",
                buf[4]
            )));
        }

        let body = &buf[..buf.len() - 4];
        let want = get_u32(buf, buf.len() - 4);
        let got = crc32c::pg_comp_crc32c(0xffff_ffff, body) ^ 0xffff_ffff;
        if want != got {
            return Err(bad("checksum mismatch"));
        }

        let seq = get_u64(buf, 8);
        let base_run_id = get_u64(buf, 16);
        let count = get_u32(buf, 24) as usize;
        let payload_len = get_u32(buf, 28) as usize;
        let xid = get_u32(buf, 32);
        let epoch = get_u64(buf, 36);
        // The payload must reach the checksum and stop there. Fitting inside
        // the object is not enough: a shorter length that lands on an entry
        // boundary parses cleanly and silently drops every write after it.
        if HEADER_LEN.checked_add(payload_len) != Some(buf.len() - 4) {
            return Err(bad("payload does not end at the checksum"));
        }
        let payload =
            take(buf, HEADER_LEN, payload_len).ok_or_else(|| bad("payload runs past the object"))?;

        let mut entries = Vec::new();
        let mut p = 0usize;
        while p < payload.len() {
            let klen = get_u32_checked(payload, p).ok_or_else(|| bad("truncated key length"))? as usize;
            p += 4;
            let key = take(payload, p, klen).ok_or_else(|| bad("key runs past the payload"))?.to_vec();
            p += klen;
            let tag = *payload.get(p).ok_or_else(|| bad("truncated entry tag"))?;
            p += 1;
            let vlen = get_u32_checked(payload, p).ok_or_else(|| bad("truncated value length"))? as usize;
            p += 4;
            let op = match tag {
                0 => Op::Put(
                    take(payload, p, vlen)
                        .ok_or_else(|| bad("value runs past the payload"))?
                        .to_vec(),
                ),
                1 => {
                    // A tombstone carries no value; a length here would be
                    // bytes the decoder skipped without reading.
                    if vlen != 0 {
                        return Err(bad("a delete entry with a value length"));
                    }
                    Op::Delete
                }
                t => return Err(bad(&format!("unknown entry tag {t}"))),
            };
            p += vlen;
            if entries.len() == count {
                return Err(bad("more entries than the header declares"));
            }
            entries.push(Entry { key, op });
        }
        if entries.len() != count {
            return Err(bad("fewer entries than the header declares"));
        }
        // Sorted, unique keys are a precondition of `lookup` and `prefixed`,
        // and they come from the BTreeMap the entries were built from.
        if entries.windows(2).any(|w| w[0].key >= w[1].key) {
            return Err(bad("entries are not in sorted key order"));
        }
        Ok(Commit { seq, base_run_id, xid, epoch, entries })
    }

    /// The CRC32C an encoded copy of this commit ends in: a fingerprint a
    /// discard marker can carry, so the marker is matched to the object it
    /// judges rather than to whatever later lands under the same number.
    pub fn fingerprint(encoded: &[u8]) -> u32 {
        get_u32_checked(encoded, encoded.len().saturating_sub(4)).unwrap_or(0)
    }

    /// Latest write for `key` in this commit, or `None` if untouched.
    /// Entries are sorted by key -- they come out of a `BTreeMap` and are
    /// encoded in that order -- so a lookup is a binary search rather than a
    /// walk. A commit can hold hundreds of thousands of entries after a bulk
    /// load, and this runs once per row read.
    pub fn lookup(&self, key: &[u8]) -> Option<&Op> {
        self.entries
            .binary_search_by(|e| e.key.as_slice().cmp(key))
            .ok()
            .map(|i| &self.entries[i].op)
    }

    /// The entries in `[lo, hi)`. Sorted, so both ends are a binary search.
    pub fn ranged(&self, lo: &[u8], hi: &[u8]) -> &[Entry] {
        let start = self.entries.partition_point(|e| e.key.as_slice() < lo);
        let end = self.entries.partition_point(|e| e.key.as_slice() < hi);
        &self.entries[start..end.max(start)]
    }

    /// Every entry whose key starts with `prefix`, in key order.
    pub fn prefixed(&self, prefix: &[u8]) -> &[Entry] {
        let start = self
            .entries
            .partition_point(|e| e.key.as_slice() < prefix);
        let len = self.entries[start..]
            .iter()
            .take_while(|e| e.key.starts_with(prefix))
            .count();
        &self.entries[start..start + len]
    }
}

/// One object carrying every commit in `commits`, under the first one's key.
///
/// Each member keeps its own sequence number and header, so what the reader
/// applies is a run of ordinary commits; only the PUT is shared. The members'
/// numbers need not be contiguous: one can be dropped between staging and the
/// write, and a gap in the numbering is not an error anywhere.
pub fn encode_batch(commits: &[Commit]) -> Vec<u8> {
    debug_assert!(commits.windows(2).all(|w| w[0].seq < w[1].seq));
    let encoded: Vec<Vec<u8>> = commits.iter().map(Commit::encode).collect();
    let members: Vec<&[u8]> = encoded.iter().map(Vec::as_slice).collect();
    encode_batch_members(&members)
}

/// The same from members already encoded: the writer encodes each commit
/// once, at staging, and the fingerprint a discard marker carries is of
/// those exact bytes.
pub fn encode_batch_members(members: &[&[u8]]) -> Vec<u8> {
    debug_assert!(!members.is_empty());
    let mut out = Vec::new();
    put_u32(&mut out, BATCH_MAGIC);
    out.push(BATCH_VERSION);
    out.extend_from_slice(&[0, 0, 0]);
    put_u32(&mut out, members.len() as u32);
    debug_assert_eq!(out.len(), BATCH_HEADER_LEN);
    for bytes in members {
        put_u32(&mut out, bytes.len() as u32);
        out.extend_from_slice(bytes);
    }
    let crc = crc32c::pg_comp_crc32c(0xffff_ffff, &out) ^ 0xffff_ffff;
    put_u32(&mut out, crc);
    out
}

/// The commits in one object, whichever form it takes. In ascending sequence
/// order, which a batch is required to keep.
pub fn decode_object(buf: &[u8]) -> io::Result<Vec<Commit>> {
    Ok(decode_members(buf)?.into_iter().map(|(c, _)| c).collect())
}

/// `decode_object`, with each member's fingerprint (see
/// [`Commit::fingerprint`]) so the open can match discard markers to them.
pub fn decode_members(buf: &[u8]) -> io::Result<Vec<(Commit, u32)>> {
    fn bad(what: &str) -> io::Error {
        io::Error::other(format!("malformed batch object: {what}"))
    }
    if buf.len() < 4 {
        return Err(bad("shorter than a magic number"));
    }
    if get_u32(buf, 0) != BATCH_MAGIC {
        return Ok(vec![(Commit::decode(buf)?, Commit::fingerprint(buf))]);
    }
    if buf.len() < BATCH_HEADER_LEN + 4 {
        return Err(bad("shorter than a header"));
    }
    if buf[4] != BATCH_VERSION {
        return Err(bad(&format!("unsupported version {}", buf[4])));
    }
    let body = &buf[..buf.len() - 4];
    let want = get_u32(buf, buf.len() - 4);
    let got = crc32c::pg_comp_crc32c(0xffff_ffff, body) ^ 0xffff_ffff;
    if want != got {
        return Err(bad("checksum mismatch"));
    }
    let count = get_u32(buf, 8) as usize;
    let mut out = Vec::with_capacity(count.min(1024));
    let mut p = BATCH_HEADER_LEN;
    while p < body.len() {
        let len = get_u32_checked(body, p).ok_or_else(|| bad("truncated member length"))? as usize;
        p += 4;
        let member = body
            .get(p..p.checked_add(len).ok_or_else(|| bad("member length overflows"))?)
            .ok_or_else(|| bad("member runs past the object"))?;
        p += len;
        if out.len() == count {
            return Err(bad("more members than the header declares"));
        }
        out.push((Commit::decode(member)?, Commit::fingerprint(member)));
    }
    if out.len() != count {
        return Err(bad("fewer members than the header declares"));
    }
    if out.windows(2).any(|w| w[0].0.seq >= w[1].0.seq) {
        return Err(bad("members are not in sequence order"));
    }
    Ok(out)
}

/// `get_u32` that returns `None` instead of panicking past the end.
pub(crate) fn get_u32_checked(b: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(b.get(at..at + 4)?.try_into().ok()?))
}

pub(crate) fn put_u32(v: &mut Vec<u8>, x: u32) {
    v.extend_from_slice(&x.to_le_bytes());
}
pub(crate) fn put_u64(v: &mut Vec<u8>, x: u64) {
    v.extend_from_slice(&x.to_le_bytes());
}
pub(crate) fn get_u32(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().unwrap())
}
pub(crate) fn get_u64(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Commit {
        Commit {
            seq: 123,
            base_run_id: 5,
            xid: 4242,
            epoch: 6,
            // Sorted and unique, as a commit built from a BTreeMap always is.
            entries: vec![
                Entry { key: b"alpha".to_vec(), op: Op::Put(b"one".to_vec()) },
                Entry { key: b"alpha/child".to_vec(), op: Op::Put(b"two".to_vec()) },
                Entry { key: b"beta".to_vec(), op: Op::Delete },
            ],
        }
    }

    #[test]
    fn round_trips() {
        let c = sample();
        assert_eq!(Commit::decode(&c.encode()).unwrap(), c);
    }

    #[test]
    fn empty_commit_round_trips() {
        let c = Commit {
            seq: 1,
            base_run_id: 0,
            xid: 1,
            epoch: 0,
            entries: vec![],
        };
        assert_eq!(Commit::decode(&c.encode()).unwrap(), c);
    }

    #[test]
    fn lookup_finds_keys_and_misses_absent_ones() {
        assert_eq!(sample().lookup(b"alpha"), Some(&Op::Put(b"one".to_vec())));
        assert_eq!(sample().lookup(b"beta"), Some(&Op::Delete));
        assert_eq!(sample().lookup(b"gamma"), None);
        assert_eq!(sample().lookup(b"al"), None, "a prefix is not a key");
    }

    #[test]
    fn prefixed_returns_the_matching_run() {
        let c = sample();
        let keys: Vec<&[u8]> = c.prefixed(b"alpha").iter().map(|e| e.key.as_slice()).collect();
        assert_eq!(keys, vec![&b"alpha"[..], &b"alpha/child"[..]]);
        assert_eq!(c.prefixed(b"beta").len(), 1);
        assert!(c.prefixed(b"zzz").is_empty());
        assert_eq!(c.prefixed(b"").len(), 3);
    }

    #[test]
    fn out_of_order_entries_are_rejected() {
        // Binary search would answer wrongly rather than loudly, so a commit
        // object whose entries are not sorted is treated as corrupt.
        let mut c = sample();
        c.entries.swap(0, 2);
        let bytes = c.encode();
        let err = Commit::decode(&bytes).unwrap_err();
        assert!(err.to_string().contains("sorted"), "{err}");
    }

    #[test]
    fn detects_corruption() {
        let mut bytes = sample().encode();
        let n = bytes.len();
        bytes[n / 2] ^= 0xff;
        assert!(Commit::decode(&bytes).is_err());
    }

    #[test]
    fn keys_sort_in_sequence_order() {
        assert!(key_for(9) < key_for(10));
        assert!(key_for(0xff) < key_for(0x100));
    }

    #[test]
    fn a_batch_round_trips_and_a_plain_commit_reads_as_a_batch_of_one() {
        let mut a = sample();
        let mut b = sample();
        b.seq = 125; // a gap is allowed: 124 was dropped before the write
        b.entries.pop();
        a.seq = 123;
        let bytes = encode_batch(&[a.clone(), b.clone()]);
        assert_eq!(decode_object(&bytes).unwrap(), vec![a.clone(), b]);
        assert_eq!(decode_object(&a.encode()).unwrap(), vec![a]);
    }

    #[test]
    fn a_batch_out_of_order_or_corrupted_is_refused() {
        let mut a = sample();
        let mut b = sample();
        a.seq = 5;
        b.seq = 4;
        // Built by hand: encode_batch asserts the order in debug builds.
        let mut out = Vec::new();
        put_u32(&mut out, BATCH_MAGIC);
        out.push(BATCH_VERSION);
        out.extend_from_slice(&[0, 0, 0]);
        put_u32(&mut out, 2);
        for c in [&a, &b] {
            let bytes = c.encode();
            put_u32(&mut out, bytes.len() as u32);
            out.extend_from_slice(&bytes);
        }
        let crc = crc32c::pg_comp_crc32c(0xffff_ffff, &out) ^ 0xffff_ffff;
        put_u32(&mut out, crc);
        assert!(decode_object(&out).unwrap_err().to_string().contains("sequence order"));

        let good = encode_batch(&[b.clone(), a.clone()]);
        let mut flipped = good.clone();
        let n = flipped.len();
        flipped[n / 2] ^= 0xff;
        assert!(decode_object(&flipped).is_err());
        for n in 0..good.len() {
            let _ = decode_object(&good[..n]); // never panics
        }
    }

    #[test]
    fn a_fingerprint_names_the_bytes_and_nothing_else() {
        let a = sample().encode();
        let mut b = sample();
        b.xid += 1;
        let b = b.encode();
        assert_eq!(Commit::fingerprint(&a), Commit::fingerprint(&sample().encode()));
        assert_ne!(Commit::fingerprint(&a), Commit::fingerprint(&b));
        let members = decode_members(&encode_batch(&[sample()])).unwrap();
        assert_eq!(members[0].1, Commit::fingerprint(&a), "a batch member keeps its own fingerprint");
    }

    #[test]
    fn an_older_format_is_refused_by_name() {
        let mut b = sample().encode();
        b[4] = 1;
        let n = b.len();
        let crc = crc32c::pg_comp_crc32c(0xffff_ffff, &b[..n - 4]) ^ 0xffff_ffff;
        b[n - 4..].copy_from_slice(&crc.to_le_bytes());
        let err = Commit::decode(&b).unwrap_err().to_string();
        assert!(err.contains("unsupported version 1"), "{err}");
    }

    #[test]
    fn a_delete_with_a_value_length_is_corrupt() {
        let c = Commit { entries: vec![Entry { key: b"k".to_vec(), op: Op::Delete }], ..sample() };
        let mut b = c.encode();
        // The delete's vlen field sits right after its tag byte.
        let at = HEADER_LEN + 4 + 1 + 1;
        assert_eq!(b[at - 1], 1, "the tag byte before it says delete");
        b[at..at + 4].copy_from_slice(&3u32.to_le_bytes());
        let n = b.len();
        let crc = crc32c::pg_comp_crc32c(0xffff_ffff, &b[..n - 4]) ^ 0xffff_ffff;
        b[n - 4..].copy_from_slice(&crc.to_le_bytes());
        let err = Commit::decode(&b).unwrap_err().to_string();
        assert!(err.contains("delete entry"), "{err}");
    }

    #[test]
    fn decode_never_panics_on_malformed_input() {
        let good = sample().encode();

        // Truncation at every length, including mid-header and mid-entry.
        for n in 0..good.len() {
            let _ = Commit::decode(&good[..n]);
        }

        // Length fields that lie. Each is re-checksummed so the CRC does not
        // reject it first -- the point is that the *parser* holds, not that
        // the checksum happens to catch it.
        let relie = |off: usize, val: u32| {
            let mut b = good.clone();
            b[off..off + 4].copy_from_slice(&val.to_le_bytes());
            let n = b.len();
            let crc = crc32c::pg_comp_crc32c(0xffff_ffff, &b[..n - 4]) ^ 0xffff_ffff;
            b[n - 4..].copy_from_slice(&crc.to_le_bytes());
            b
        };
        for (off, val) in [
            (24, u32::MAX),  // entry count
            (28, u32::MAX),  // payload length
            (28, 0),
            (24, 0),
        ] {
            let err = Commit::decode(&relie(off, val)).unwrap_err().to_string();
            assert!(err.contains("malformed commit object"), "got: {err}");
        }

        // A key length that runs past the payload.
        let mut b = good.clone();
        b[HEADER_LEN..HEADER_LEN + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        let n = b.len();
        let crc = crc32c::pg_comp_crc32c(0xffff_ffff, &b[..n - 4]) ^ 0xffff_ffff;
        b[n - 4..].copy_from_slice(&crc.to_le_bytes());
        assert!(Commit::decode(&b).is_err());

        // Random noise of every plausible length.
        for len in [0usize, 1, 8, HEADER_LEN, HEADER_LEN + 4, 100] {
            let noise: Vec<u8> = (0..len).map(|i| (i * 37 + 11) as u8).collect();
            let _ = Commit::decode(&noise);
        }
    }

}
