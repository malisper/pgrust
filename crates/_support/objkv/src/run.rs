//! Sorted runs: `run/<id>`, written by compaction, read by ranged GET.
//!
//! Layout — bloom and index are adjacent on purpose, so opening a run costs two
//! ranged GETs (trailer, then bloom+index together) and every subsequent point
//! read costs at most one more:
//!
//! ```text
//! [block 0][block 1]...[bloom][index][trailer]
//! ```
//!
//! A block is ~64KB of sorted entries. The index holds each block's first key,
//! so a lookup binary-searches in memory and fetches exactly one block.

use std::collections::HashMap;
use std::io;
use std::sync::Mutex;

use crate::bloom::Bloom;
use crate::commit::{get_u32, get_u64, put_u32, put_u64, Op};
use crate::key;

pub const MAGIC: u32 = 0x4f4b_5232; // "OKR2"
pub const TRAILER_LEN: u64 = 44;
pub const TARGET_BLOCK_BYTES: usize = 64 * 1024;
/// Default local block cache. Stands in for the NVMe cache a real deployment
/// would keep; without it every point read is a network round trip and the
/// warm-read threshold is unmeasurable.
pub const DEFAULT_CACHE_BYTES: usize = 64 * 1024 * 1024;

pub fn key_for(id: u64) -> String {
    format!("run/{id:016x}")
}

/// A delta run holds only the commits since the run before it, where a run
/// without the suffix holds everything up to its number.
pub const DELTA_SUFFIX: &str = ".d";

pub fn delta_key_for(id: u64) -> String {
    format!("run/{id:016x}{DELTA_SUFFIX}")
}

pub fn is_delta(key: &str) -> bool {
    key.ends_with(DELTA_SUFFIX)
}

/// Anything a run can be read from: an S3 object, or a byte slice in tests.
pub trait RangeSource {
    fn range(&self, offset: u64, len: u64) -> io::Result<Vec<u8>>;
    fn size(&self) -> u64;
}

impl RangeSource for &[u8] {
    fn range(&self, offset: u64, len: u64) -> io::Result<Vec<u8>> {
        let (s, e) = (offset as usize, (offset + len) as usize);
        if e > self.len() {
            return Err(io::Error::other("range past end of object"));
        }
        Ok(self[s..e].to_vec())
    }
    fn size(&self) -> u64 {
        self.len() as u64
    }
}

/// Serialise sorted `(key, op)` pairs into a run object.
///
/// Entries must already be sorted by key and deduplicated; compaction owns
/// that, not this function.
pub fn build(entries: &[(Vec<u8>, Op)]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut index: Vec<(Vec<u8>, u64, u32)> = Vec::new();

    let mut i = 0usize;
    while i < entries.len() {
        let block_off = out.len() as u64;
        let first_key = entries[i].0.clone();
        let start = out.len();
        while i < entries.len() && out.len() - start < TARGET_BLOCK_BYTES {
            let (k, op) = &entries[i];
            put_u32(&mut out, k.len() as u32);
            out.extend_from_slice(k);
            match op {
                Op::Put(v) => {
                    out.push(0);
                    put_u32(&mut out, v.len() as u32);
                    out.extend_from_slice(v);
                }
                Op::Delete => {
                    out.push(1);
                    put_u32(&mut out, 0);
                }
            }
            i += 1;
        }
        index.push((first_key, block_off, (out.len() - start) as u32));
    }

    // Bloom over row keys: a seek knows the row, not the version.
    let keys: Vec<&[u8]> = entries
        .iter()
        .map(|(k, _)| key::row_of(k).unwrap_or(k.as_slice()))
        .collect();
    let bloom = Bloom::build(&keys);
    let bloom_off = out.len() as u64;
    out.extend_from_slice(bloom.as_bytes());
    let bloom_len = (out.len() as u64 - bloom_off) as u32;

    let index_off = out.len() as u64;
    for (k, off, len) in &index {
        put_u32(&mut out, k.len() as u32);
        out.extend_from_slice(k);
        put_u64(&mut out, *off);
        put_u32(&mut out, *len);
    }
    let index_len = (out.len() as u64 - index_off) as u32;

    let meta_crc = crc32c::pg_comp_crc32c(
        0xffff_ffff,
        &out[bloom_off as usize..(index_off + index_len as u64) as usize],
    ) ^ 0xffff_ffff;

    put_u64(&mut out, bloom_off);
    put_u32(&mut out, bloom_len);
    put_u64(&mut out, index_off);
    put_u32(&mut out, index_len);
    put_u32(&mut out, index.len() as u32);
    put_u64(&mut out, entries.len() as u64);
    put_u32(&mut out, meta_crc);
    put_u32(&mut out, MAGIC);
    out
}

/// An opened run: bloom and index held locally, blocks fetched on demand.
pub struct Run<S: RangeSource> {
    src: S,
    bloom: Bloom,
    /// (first_key, block_offset, block_len), ascending by first_key.
    index: Vec<(Vec<u8>, u64, u32)>,
    pub entry_count: u64,
    cache: Mutex<BlockCache>,
}

/// FIFO block cache. Not an LRU — the access pattern under test is uniform
/// random, where the two behave the same and FIFO is simpler to reason about.
#[derive(Default)]
struct BlockCache {
    blocks: HashMap<u64, Vec<u8>>,
    order: std::collections::VecDeque<u64>,
    bytes: usize,
    cap: usize,
    pub hits: u64,
    pub misses: u64,
}

impl BlockCache {
    fn get(&mut self, off: u64) -> Option<Vec<u8>> {
        match self.blocks.get(&off) {
            Some(b) => {
                self.hits += 1;
                Some(b.clone())
            }
            None => {
                self.misses += 1;
                None
            }
        }
    }
    fn insert(&mut self, off: u64, block: Vec<u8>) {
        if self.cap == 0 || block.len() > self.cap {
            return;
        }
        while self.bytes + block.len() > self.cap {
            let Some(old) = self.order.pop_front() else { break };
            if let Some(b) = self.blocks.remove(&old) {
                self.bytes -= b.len();
            }
        }
        self.bytes += block.len();
        self.order.push_back(off);
        self.blocks.insert(off, block);
    }
}

impl<S: RangeSource> Run<S> {
    /// Two ranged GETs: the trailer, then bloom and index in one call.
    pub fn open(src: S) -> io::Result<Run<S>> {
        let size = src.size();
        if size < TRAILER_LEN {
            return Err(io::Error::other("object too small to be a run"));
        }
        let t = src.range(size - TRAILER_LEN, TRAILER_LEN)?;
        if get_u32(&t, 40) != MAGIC {
            return Err(io::Error::other("bad run magic"));
        }
        let bloom_off = get_u64(&t, 0);
        let bloom_len = get_u32(&t, 8) as u64;
        let index_off = get_u64(&t, 12);
        let index_len = get_u32(&t, 20) as u64;
        // The single bloom+index fetch below depends on them being adjacent.
        if index_off != bloom_off + bloom_len {
            return Err(io::Error::other("run metadata is not contiguous"));
        }
        let entry_count = get_u64(&t, 28);
        let want_crc = get_u32(&t, 36);

        let meta = src.range(bloom_off, bloom_len + index_len)?;
        let got_crc = crc32c::pg_comp_crc32c(0xffff_ffff, &meta) ^ 0xffff_ffff;
        if want_crc != got_crc {
            return Err(io::Error::other("run metadata checksum mismatch"));
        }

        let bloom = Bloom::from_bytes(meta[..bloom_len as usize].to_vec());
        let ibytes = &meta[bloom_len as usize..];
        let mut index = Vec::new();
        let mut p = 0usize;
        while p < ibytes.len() {
            let klen = get_u32(ibytes, p) as usize;
            p += 4;
            let key = ibytes[p..p + klen].to_vec();
            p += klen;
            let off = get_u64(ibytes, p);
            p += 8;
            let len = get_u32(ibytes, p);
            p += 4;
            index.push((key, off, len));
        }
        Ok(Run {
            src,
            bloom,
            index,
            entry_count,
            cache: Mutex::new(BlockCache { cap: DEFAULT_CACHE_BYTES, ..Default::default() }),
        })
    }

    /// Where the run is read from: the object it lives in.
    pub fn source(&self) -> &S {
        &self.src
    }

    /// The sequence number of the version live at `snapshot`.
    pub fn seq_at(&self, row_key: &[u8], snapshot: u64) -> io::Result<Option<u64>> {
        Ok(self.locate_at(row_key, snapshot)?.and_then(|(k, _)| key::seq_of(&k)))
    }

    /// The version of `row_key` live at `snapshot`. At most one ranged GET.
    pub fn get_at(&self, row_key: &[u8], snapshot: u64) -> io::Result<Option<Op>> {
        Ok(self.locate_at(row_key, snapshot)?.map(|(_, op)| op))
    }

    /// One block, from the cache or one ranged GET.
    fn block_at(&self, off: u64, len: u32) -> io::Result<Vec<u8>> {
        // Scoped: inlining this into a match holds the guard across the miss
        // arm and self-deadlocks.
        let cached = self.cache.lock().unwrap().get(off);
        match cached {
            Some(b) => Ok(b),
            None => {
                let b = self.src.range(off, len as u64)?;
                self.cache.lock().unwrap().insert(off, b.clone());
                Ok(b)
            }
        }
    }

    /// Every entry whose key starts with `prefix`, in key order.
    ///
    /// Seeks rather than scans: the sparse index picks the first block that
    /// can hold the prefix, and the walk stops at the first key past it. This
    /// is what makes an index lookup cost a couple of ranged GETs instead of a
    /// read of the whole run -- the difference between an index being worth
    /// having and not.
    /// Every stored key in `[lo, hi)`, in order.
    ///
    /// Bounds are plain byte strings and the range is half-open, so "greater
    /// than this value" and "up to and including it" are both expressed by
    /// where the caller puts the bound rather than by a flag here.
    pub fn scan_range(&self, lo: &[u8], hi: &[u8]) -> io::Result<Vec<(Vec<u8>, Op)>> {
        self.scan_range_limited(lo, hi, crate::key::LATEST, usize::MAX)
    }

    /// The same, stopping once `limit` distinct rows have been seen.
    ///
    /// Stored keys carry a version suffix, so one row can appear several
    /// times; the limit counts rows. Taking the first `limit` from each layer
    /// and merging afterwards is safe: a key among the first `limit` of the
    /// union is among the first `limit` of whichever layer holds it.
    ///
    /// Versions above `snapshot` are left out here rather than by the caller.
    /// Counted, they would fill the page with rows the snapshot cannot see,
    /// and a page that came back short would be mistaken for the end of the
    /// range.
    pub fn scan_range_limited(
        &self,
        lo: &[u8],
        hi: &[u8],
        snapshot: u64,
        limit: usize,
    ) -> io::Result<Vec<(Vec<u8>, Op)>> {
        if self.index.is_empty() || lo >= hi || limit == 0 {
            return Ok(Vec::new());
        }
        let mut rows = 0usize;
        let mut last: Option<Vec<u8>> = None;
        let start = match self.index.binary_search_by(|(k, _, _)| k.as_slice().cmp(lo)) {
            Ok(i) => i,
            Err(0) => 0,
            Err(i) => i - 1,
        };
        let mut out = Vec::new();
        for pos in start..self.index.len() {
            let (first, off, len) = &self.index[pos];
            // Blocks are ordered: once one starts at or past the upper bound,
            // so does every block after it.
            if first.as_slice() >= hi {
                break;
            }
            let block = self.block_at(*off, *len)?;
            let mut past = false;
            for (k, op) in decode_block(&block)? {
                // The bounds name rows, and a caller resuming a page passes
                // the last row it saw plus a zero byte. That sits below the
                // row's own versions, which carry `/`, so the whole key would
                // compare above it and the row would come back twice.
                if crate::key::row_of(&k).unwrap_or(&k) < lo {
                    continue;
                }
                if k.as_slice() >= hi {
                    past = true;
                    break;
                }
                if crate::key::seq_of(&k).is_some_and(|seq| seq > snapshot) {
                    continue;
                }
                let row = crate::key::row_of(&k).map(|r| r.to_vec());
                if row.is_some() && row != last {
                    if rows == limit {
                        past = true;
                        break;
                    }
                    rows += 1;
                    last = row;
                }
                out.push((k, op));
            }
            if past {
                break;
            }
        }
        Ok(out)
    }

    /// The last `limit` rows of `[lo, hi)`, still in ascending order.
    ///
    /// Blocks are walked from the top down so a scan reading backwards stops
    /// as early as a forward one does; ORDER BY ... DESC LIMIT 10 is the whole
    /// reason it exists.
    pub fn scan_range_back(
        &self,
        lo: &[u8],
        hi: &[u8],
        snapshot: u64,
        limit: usize,
    ) -> io::Result<Vec<(Vec<u8>, Op)>> {
        if self.index.is_empty() || lo >= hi || limit == 0 {
            return Ok(Vec::new());
        }
        let mut rows = 0usize;
        let mut last: Option<Vec<u8>> = None;
        let mut out: Vec<(Vec<u8>, Op)> = Vec::new();
        for pos in (0..self.index.len()).rev() {
            let (first, off, len) = &self.index[pos];
            // Once a block starts at or past the top of the range, nothing in
            // it is wanted; once one starts below the bottom, this is the last.
            if first.as_slice() >= hi {
                continue;
            }
            let block = self.block_at(*off, *len)?;
            let mut done = false;
            let entries = decode_block(&block)?;
            for (k, op) in entries.into_iter().rev() {
                if k.as_slice() >= hi {
                    continue;
                }
                if crate::key::row_of(&k).unwrap_or(&k) < lo {
                    done = true;
                    break;
                }
                if crate::key::seq_of(&k).is_some_and(|seq| seq > snapshot) {
                    continue;
                }
                let row = crate::key::row_of(&k).map(|r| r.to_vec());
                if row.is_some() && row != last {
                    if rows == limit {
                        done = true;
                        break;
                    }
                    rows += 1;
                    last = row;
                }
                out.push((k, op));
            }
            if done || first.as_slice() <= lo {
                break;
            }
        }
        out.reverse();
        Ok(out)
    }

    pub fn scan_prefix(&self, prefix: &[u8]) -> io::Result<Vec<(Vec<u8>, Op)>> {
        if self.index.is_empty() {
            return Ok(Vec::new());
        }
        let start = match self.index.binary_search_by(|(k, _, _)| k.as_slice().cmp(prefix)) {
            Ok(i) => i,
            Err(0) => 0,
            Err(i) => i - 1,
        };
        let mut out = Vec::new();
        for pos in start..self.index.len() {
            let (first, off, len) = &self.index[pos];
            // Blocks are ordered, so once one starts past the prefix, every
            // block after it does too.
            if first.as_slice() > prefix && !first.starts_with(prefix) {
                break;
            }
            let block = self.block_at(*off, *len)?;
            let mut past = false;
            for (k, op) in decode_block(&block)? {
                if k.as_slice() < prefix {
                    continue;
                }
                if !k.starts_with(prefix) {
                    past = true;
                    break;
                }
                out.push((k, op));
            }
            if past {
                break;
            }
        }
        Ok(out)
    }

    /// The same as `locate_at`, named for callers outside this module that
    /// need the versioned key as well as the value.
    pub fn locate_stamped_at(
        &self,
        row_key: &[u8],
        snapshot: u64,
    ) -> io::Result<Option<(Vec<u8>, Op)>> {
        self.locate_at(row_key, snapshot)
    }

    fn locate_at(&self, row_key: &[u8], snapshot: u64) -> io::Result<Option<(Vec<u8>, Op)>> {
        if !self.bloom.may_contain(row_key) || self.index.is_empty() {
            return Ok(None);
        }
        let probe = key::seek_at(row_key, snapshot);
        let start = match self.index.binary_search_by(|(k, _, _)| k.as_slice().cmp(&probe)) {
            Ok(i) => i,
            Err(0) => 0,
            Err(i) => i - 1,
        };
        // The wanted entry may be the first entry of the next block.
        for pos in start..self.index.len().min(start + 2) {
            let (_, off, len) = &self.index[pos];
            let block = self.block_at(*off, *len)?;
            if let Some((found, op)) = seek_block(&block, &probe)? {
                return Ok(key::belongs_to(&found, row_key).then_some((found, op)));
            }
        }
        Ok(None)
    }

    pub fn block_count(&self) -> usize {
        self.index.len()
    }

    /// (hits, misses) against the local block cache.
    pub fn cache_stats(&self) -> (u64, u64) {
        let c = self.cache.lock().unwrap();
        (c.hits, c.misses)
    }

    pub fn set_cache_bytes(&self, cap: usize) {
        self.cache.lock().unwrap().cap = cap;
    }

    /// Every entry in key order, tombstones included. Used by compaction, so it
    /// fetches whole blocks rather than ranges — the one read path that is
    /// allowed to be expensive.
    pub fn scan(&self) -> io::Result<Vec<(Vec<u8>, Op)>> {
        // No with_capacity on entry_count: it comes off the trailer, which no
        // checksum covers, and a wrong value here asks the allocator for it.
        let mut out = Vec::new();
        for (_, off, len) in &self.index {
            out.extend(decode_block(&self.src.range(*off, *len as u64)?)?);
        }
        Ok(out)
    }
}

/// Every entry in one block, in key order.
///
/// Fallible, and bounds-checked at every step. A run object arrives over the
/// network like a commit object does, and `commit::decode` already states the
/// rule: length fields may disagree with the buffer they came in. Only the
/// bloom and the block index are covered by `meta_crc`; the blocks and the
/// trailer are not. So a short ranged GET, a truncated upload or a flipped
/// bit reaches here as a length that runs off the end, and unchecked slicing
/// would take the backend down rather than report a bad object.
fn decode_block(block: &[u8]) -> io::Result<Vec<(Vec<u8>, Op)>> {
    let mut out = Vec::new();
    let mut p = 0usize;
    while p + 4 <= block.len() {
        let (k, tag, value, next) = entry_at(block, p)?;
        out.push((k.to_vec(), if tag == 0 { Op::Put(value.to_vec()) } else { Op::Delete }));
        p = next;
    }
    Ok(out)
}

/// First entry at or after `probe` within one block, if any.
fn seek_block(block: &[u8], probe: &[u8]) -> io::Result<Option<(Vec<u8>, Op)>> {
    let mut p = 0usize;
    while p + 4 <= block.len() {
        let (k, tag, value, next) = entry_at(block, p)?;
        if k >= probe {
            return Ok(Some((
                k.to_vec(),
                if tag == 0 { Op::Put(value.to_vec()) } else { Op::Delete },
            )));
        }
        p = next;
    }
    Ok(None)
}

/// One encoded entry at `p`: key, tag, value, and where the next one starts.
fn entry_at(block: &[u8], p: usize) -> io::Result<(&[u8], u8, &[u8], usize)> {
    fn short<T>() -> io::Result<T> {
        Err(io::Error::new(io::ErrorKind::InvalidData, "objkv: truncated run block"))
    }
    let take = |at: usize, n: usize| -> io::Result<(&[u8], usize)> {
        match block.get(at..at + n) {
            Some(s) => Ok((s, at + n)),
            None => short(),
        }
    };
    let (klen_bytes, p) = take(p, 4)?;
    let klen = u32::from_le_bytes(klen_bytes.try_into().expect("4 bytes")) as usize;
    let (k, p) = take(p, klen)?;
    let (tag_byte, p) = take(p, 1)?;
    let (vlen_bytes, p) = take(p, 4)?;
    let vlen = u32::from_le_bytes(vlen_bytes.try_into().expect("4 bytes")) as usize;
    let (value, p) = take(p, vlen)?;
    Ok((k, tag_byte[0], value, p))
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::LATEST;

    /// Counts ranged reads so tests can assert the round-trip budget, which is
    /// the whole point of the layout.
    struct Counting<'a> {
        bytes: &'a [u8],
        reads: std::cell::Cell<usize>,
    }
    impl RangeSource for Counting<'_> {
        fn range(&self, offset: u64, len: u64) -> io::Result<Vec<u8>> {
            self.reads.set(self.reads.get() + 1);
            self.bytes.range(offset, len)
        }
        fn size(&self) -> u64 {
            self.bytes.len() as u64
        }
    }

    fn row(i: usize) -> Vec<u8> {
        format!("key{i:08}").into_bytes()
    }

    /// One version of each row, all stamped at seq 1.
    fn entries(n: usize) -> Vec<(Vec<u8>, Op)> {
        let mut e: Vec<(Vec<u8>, Op)> = (0..n)
            .map(|i| (key::versioned(&row(i), 1), Op::Put(vec![b'v'; 100])))
            .collect();
        e.sort_by(|a, b| a.0.cmp(&b.0));
        e
    }

    #[test]
    fn finds_every_key_it_stored() {
        let e = entries(5000);
        let bytes = build(&e);
        let run = Run::open(bytes.as_slice()).unwrap();
        assert_eq!(run.entry_count, 5000);
        assert!(run.block_count() > 1, "should span several blocks");
        for (k, v) in &e {
            let r = key::row_of(k).unwrap();
            assert_eq!(run.get_at(r, LATEST).unwrap().as_ref(), Some(v));
        }
    }

    #[test]
    fn point_read_costs_one_ranged_get_after_open() {
        let bytes = build(&entries(5000));
        let src = Counting { bytes: bytes.as_slice(), reads: std::cell::Cell::new(0) };
        let run = Run::open(src).unwrap();
        run.set_cache_bytes(0); // measure raw GETs, not cache behaviour
        // open() = trailer + (bloom+index) = 2.
        assert_eq!(run.src.reads.get(), 2);
        run.get_at(b"key00002500", LATEST).unwrap();
        assert_eq!(run.src.reads.get(), 3, "a hit must cost exactly one more GET");
    }

    #[test]
    fn absent_keys_usually_cost_nothing() {
        let bytes = build(&entries(2000));
        let src = Counting { bytes: bytes.as_slice(), reads: std::cell::Cell::new(0) };
        let run = Run::open(src).unwrap();
        let before = run.src.reads.get();
        for i in 0..500 {
            let k = format!("absent{i:08}");
            assert!(run.get_at(k.as_bytes(), LATEST).unwrap().is_none());
        }
        let fetched = run.src.reads.get() - before;
        // Bloom should reject nearly all of them without touching a block.
        assert!(fetched < 50, "bloom let {fetched}/500 misses through to a GET");
    }

    #[test]
    fn cache_turns_a_repeat_read_into_zero_gets() {
        let bytes = build(&entries(5000));
        let src = Counting { bytes: bytes.as_slice(), reads: std::cell::Cell::new(0) };
        let run = Run::open(src).unwrap();
        let k = b"key00002500";
        run.get_at(k, LATEST).unwrap();
        let after_first = run.src.reads.get();
        for _ in 0..100 {
            run.get_at(k, LATEST).unwrap();
        }
        assert_eq!(run.src.reads.get(), after_first, "repeat reads must be free");
        let (hits, misses) = run.cache_stats();
        assert_eq!((hits, misses), (100, 1));
    }

    #[test]
    fn a_prefix_scan_seeks_instead_of_reading_the_run() {
        // The property an index lookup lives or dies on. A run holding many
        // rows must answer "everything under this prefix" with a couple of
        // ranged GETs, not one per block.
        let mut e: Vec<(Vec<u8>, Op)> = Vec::new();
        for i in 0..20_000usize {
            // Two prefixes, interleaved so neither is contiguous by accident.
            let p = if i % 2 == 0 { "aaa" } else { "bbb" };
            e.push((
                key::versioned(format!("{p}/{i:08}").as_bytes(), 1),
                Op::Put(vec![b'v'; 60]),
            ));
        }
        e.sort_by(|a, b| a.0.cmp(&b.0));
        let bytes = build(&e);
        let src = Counting { bytes: &bytes, reads: Default::default() };
        let run = Run::open(src).unwrap();
        assert!(run.block_count() > 10, "needs enough blocks for the test to mean anything");

        let before = run.src.reads.get();
        let hits = run.scan_prefix(b"aaa/00000042").unwrap();
        let reads = run.src.reads.get() - before;
        assert_eq!(hits.len(), 1);
        assert!(reads <= 2, "a point prefix cost {reads} ranged GETs, not a seek");

        // A wide prefix reads only its own share of the run.
        let before = run.src.reads.get();
        let all_a = run.scan_prefix(b"aaa/").unwrap();
        let reads = run.src.reads.get() - before;
        assert_eq!(all_a.len(), 10_000);
        assert!(
            reads < run.block_count(),
            "reading half the keys touched {reads} of {} blocks",
            run.block_count()
        );

        assert!(run.scan_prefix(b"zzz/").unwrap().is_empty());
        assert_eq!(run.scan_prefix(b"").unwrap().len(), 20_000);
    }

    #[test]
    fn scan_returns_everything_in_key_order() {
        let e = entries(3000);
        let bytes = build(&e);
        let run = Run::open(bytes.as_slice()).unwrap();
        let got = run.scan().unwrap();
        assert_eq!(got.len(), e.len());
        assert!(got.windows(2).all(|w| w[0].0 < w[1].0), "scan must be sorted");
        assert_eq!(got[0].0, e[0].0);
    }

    #[test]
    fn preserves_tombstones() {
        let mut e = vec![
            (key::versioned(b"a", 1), Op::Put(b"1".to_vec())),
            (key::versioned(b"b", 1), Op::Delete),
        ];
        e.sort_by(|x, y| x.0.cmp(&y.0));
        let bytes = build(&e);
        let run = Run::open(bytes.as_slice()).unwrap();
        assert_eq!(run.get_at(b"b", LATEST).unwrap(), Some(Op::Delete));
        assert_eq!(run.get_at(b"c", LATEST).unwrap(), None);
    }

    #[test]
    fn detects_metadata_corruption() {
        let mut bytes = build(&entries(100));
        let n = bytes.len();
        bytes[n - (TRAILER_LEN as usize) - 5] ^= 0xff;
        assert!(Run::open(bytes.as_slice()).is_err());
    }

    #[test]
    fn a_run_answers_at_any_snapshot_it_holds() {
        // Three versions of one row, plus enough neighbours to span blocks, so
        // the seek has to do real work rather than land on entry zero.
        let mut e: Vec<(Vec<u8>, Op)> = Vec::new();
        for i in 0..2000 {
            e.push((key::versioned(&row(i), 1), Op::Put(vec![b'p'; 60])));
        }
        e.push((key::versioned(&row(900), 4), Op::Put(b"at-four".to_vec())));
        e.push((key::versioned(&row(900), 9), Op::Put(b"at-nine".to_vec())));
        e.push((key::versioned(&row(901), 7), Op::Delete));
        e.sort_by(|a, b| a.0.cmp(&b.0));

        let bytes = build(&e);
        let run = Run::open(bytes.as_slice()).unwrap();
        let val = |snap| match run.get_at(&row(900), snap).unwrap() {
            Some(Op::Put(v)) => String::from_utf8(v).unwrap(),
            other => panic!("unexpected {other:?}"),
        };
        assert_eq!(val(LATEST), "at-nine");
        assert_eq!(val(9), "at-nine");
        assert_eq!(val(8), "at-four", "snapshot 8 predates version 9");
        assert_eq!(val(4), "at-four");
        assert_eq!(val(3).len(), 60, "before either update, the original");
        assert_eq!(run.get_at(&row(900), 0).unwrap(), None, "before it existed");

        // A tombstone is a version like any other: visible as deleted after it,
        // and invisible before it.
        assert_eq!(run.get_at(&row(901), LATEST).unwrap(), Some(Op::Delete));
        assert!(matches!(run.get_at(&row(901), 6).unwrap(), Some(Op::Put(_))));
    }

}
