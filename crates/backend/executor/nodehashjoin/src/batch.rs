//! M3.5 join-batch spill vocabulary (docs/design/m3.5-spill.md §5; the
//! inc-4/5 "STS + PLAN-BATCHES" charter — a SharedTuplestore-EQUIVALENT on
//! the SpillSet substrate, per the ratified lane-close re-charter).
//!
//! Three pure pieces, all consumed by the execmain runtime-hashjoin wiring:
//!
//! 1. The BATCH RECORD byte contract: `[u32 ne hashvalue][u32 ne t_len]
//!    [t_len minimal-tuple bytes][zero pad to 8]`. The 8-byte header plus
//!    8-byte record alignment keep every tuple image MAXALIGNed when the
//!    reader materializes an extent into an 8-aligned buffer (the STS
//!    read-buffer discipline). Torn/overrunning records FAIL CLOSED.
//! 2. The BATCH ADDRESSING law: batch selection consumes bits of
//!    `mix64(hashvalue)` — a full-avalanche remix — top-down: level 0 takes
//!    the top `log2n` bits; a split at `consumed` bits takes the next
//!    `jbits` bits below. DEVIATION from the design doc's C bit-slicing
//!    (§5.1 item 2: "batchno = the next bits above log2_nbuckets"),
//!    recorded in notes/m35-spill.md: the doc's parity REQUIREMENT is that
//!    inner and outer agree and that children partition their parent
//!    exactly — both hold for any deterministic slicing of one avalanche
//!    hash (equal hashvalues route together; deeper slices nest strictly).
//!    The remix decorrelates batch bits from the runtime table's bucket
//!    law (top-8 partition + low bits, shared_build.rs) and from the
//!    16-bit tag-bloom bits, which C's scheme would collide with here.
//! 3. The LEAF MAP: the PLAN-BATCHES output — a trie from level-0 batches
//!    through split nodes to final leaf slots, FROZEN before the first
//!    outer row routes (§5.2: the outer side is never repartitioned).
//!
//! The serial hash-join batching (nodehash/src/lib.rs) is untouched — the
//! serial arm stays the byte-parity oracle.

use ::types_error::{PgError, PgResult, ERROR};

/// Bytes of the record header (hashvalue + t_len).
pub const BATCH_REC_HDR: usize = 8;

fn pad8(n: usize) -> usize {
    (n + 7) & !7
}

/// On-file byte length of one record carrying a `t_len`-byte tuple image.
pub fn batch_record_len(t_len: usize) -> usize {
    BATCH_REC_HDR + pad8(t_len)
}

/// Append one record to `out` (zero-padded to 8 bytes).
pub fn batch_record_push(out: &mut Vec<u8>, hashvalue: u32, tuple: &[u8]) {
    out.reserve(batch_record_len(tuple.len()));
    out.extend_from_slice(&hashvalue.to_ne_bytes());
    out.extend_from_slice(&(tuple.len() as u32).to_ne_bytes());
    out.extend_from_slice(tuple);
    let pad = pad8(tuple.len()) - tuple.len();
    out.extend_from_slice(&[0u8; 8][..pad]);
}

fn torn(what: &str) -> Box<PgError> {
    PgError::new(ERROR, format!("torn join batch spill record ({what})")).into()
}

/// Streaming decoder over one materialized extent (or any concatenation of
/// whole records). Yields `(hashvalue, tuple_bytes)`; the tuple slice
/// borrows the input buffer. When the input buffer is 8-aligned, every
/// yielded tuple slice is 8-aligned (record layout invariant). Fails closed
/// on torn tails and overrunning lengths.
pub struct BatchRecords<'a> {
    buf: &'a [u8],
    off: usize,
}

impl<'a> BatchRecords<'a> {
    pub fn new(buf: &'a [u8]) -> BatchRecords<'a> {
        BatchRecords { buf, off: 0 }
    }

    /// Next record; `None` = clean end of input.
    pub fn next_rec(&mut self) -> PgResult<Option<(u32, &'a [u8])>> {
        if self.off == self.buf.len() {
            return Ok(None);
        }
        if self.buf.len() - self.off < BATCH_REC_HDR {
            return Err(torn("short header"));
        }
        let h = u32::from_ne_bytes(self.buf[self.off..self.off + 4].try_into().unwrap());
        let t_len =
            u32::from_ne_bytes(self.buf[self.off + 4..self.off + 8].try_into().unwrap()) as usize;
        let end = self.off + BATCH_REC_HDR + pad8(t_len);
        if end > self.buf.len() {
            return Err(torn("length overruns the extent"));
        }
        let tuple = &self.buf[self.off + BATCH_REC_HDR..self.off + BATCH_REC_HDR + t_len];
        self.off = end;
        Ok(Some((h, tuple)))
    }
}

// ---------------------------------------------------------------------------
// Batch addressing: top-down slices of one avalanche remix.
// ---------------------------------------------------------------------------

/// splitmix64 finalizer (the distinctset/pardistinct mix64 family).
#[inline]
pub fn batch_mix(hashvalue: u32) -> u64 {
    let mut x = hashvalue as u64;
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

/// Level-0 batch: the TOP `log2n` bits of the remix. `log2n` in 1..=16.
#[inline]
pub fn batch_of(hashvalue: u32, log2n: u32) -> u32 {
    debug_assert!((1..=16).contains(&log2n));
    (batch_mix(hashvalue) >> (64 - log2n)) as u32
}

/// Split child: `jbits` bits at `consumed` bits below the top (level-0's
/// `log2n` counts toward `consumed`). Children of one node nest strictly:
/// they slice bits their ancestors never consumed.
#[inline]
pub fn split_child(hashvalue: u32, consumed: u32, jbits: u32) -> u32 {
    debug_assert!(jbits >= 1 && consumed + jbits <= 56, "split slice out of remix range");
    ((batch_mix(hashvalue) >> (64 - consumed - jbits)) & ((1u64 << jbits) - 1)) as u32
}

/// In-memory batch 0's leaf sentinel (probe inline against the shared
/// table; never a spill-file leaf).
pub const LEAF_INMEM: u16 = u16::MAX;
/// Placeholder for a not-yet-resolved node (must not survive the freeze).
pub const LEAF_PENDING: u16 = u16::MAX - 1;

#[derive(Clone, Copy, Debug)]
pub enum MapNode {
    Leaf(u16),
    /// Children occupy nodes `child_base .. child_base + (1 << jbits)`.
    Split { consumed: u8, jbits: u8, child_base: u32 },
}

/// The PLAN-BATCHES trie: level-0 batches → (splits) → leaf slots.
pub struct LeafMap {
    log2n: u32,
    /// Node index per level-0 batch (identity at construction: batch b is
    /// node b).
    nodes: Vec<MapNode>,
}

impl LeafMap {
    /// `n` level-0 batches (power of two ≥ 2); every batch starts PENDING.
    pub fn new(n: u32) -> LeafMap {
        assert!(n.is_power_of_two() && n >= 2);
        LeafMap {
            log2n: n.trailing_zeros(),
            nodes: (0..n).map(|_| MapNode::Leaf(LEAF_PENDING)).collect(),
        }
    }

    pub fn log2n(&self) -> u32 {
        self.log2n
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Split a LEVEL-0 batch node into `1 << jbits` children. Returns the
    /// child node base; children start PENDING.
    pub fn split_node(&mut self, node: u32, jbits: u32) -> u32 {
        debug_assert!((node as usize) < (1usize << self.log2n), "level-0 nodes only");
        debug_assert!(matches!(self.nodes[node as usize], MapNode::Leaf(LEAF_PENDING)));
        self.split_at(node, self.log2n, jbits)
    }

    /// Split a pending CHILD node whose consumed-bit depth the caller
    /// tracked (the PLAN/round bookkeeping carries it).
    pub fn split_child_node(&mut self, node: u32, consumed: u32, jbits: u32) -> u32 {
        debug_assert!(matches!(self.nodes[node as usize], MapNode::Leaf(LEAF_PENDING)));
        self.split_at(node, consumed, jbits)
    }

    fn split_at(&mut self, node: u32, consumed: u32, jbits: u32) -> u32 {
        assert!(jbits >= 1 && consumed + jbits <= 56);
        let child_base = self.nodes.len() as u32;
        for _ in 0..(1u32 << jbits) {
            self.nodes.push(MapNode::Leaf(LEAF_PENDING));
        }
        self.nodes[node as usize] = MapNode::Split {
            consumed: consumed as u8,
            jbits: jbits as u8,
            child_base,
        };
        child_base
    }

    pub fn set_leaf(&mut self, node: u32, leaf: u16) {
        debug_assert!(matches!(self.nodes[node as usize], MapNode::Leaf(LEAF_PENDING)));
        self.nodes[node as usize] = MapNode::Leaf(leaf);
    }

    /// True ⇔ every node resolved (no PENDING leaves) — the freeze gate.
    pub fn fully_resolved(&self) -> bool {
        !self
            .nodes
            .iter()
            .any(|n| matches!(n, MapNode::Leaf(l) if *l == LEAF_PENDING))
    }

    /// Route one hashvalue to its leaf slot ([`LEAF_INMEM`] = the in-memory
    /// batch-0 table). Must only run on a fully-resolved map.
    #[inline]
    pub fn resolve(&self, hashvalue: u32) -> u16 {
        let mut node = batch_of(hashvalue, self.log2n);
        loop {
            match self.nodes[node as usize] {
                MapNode::Leaf(l) => {
                    debug_assert_ne!(l, LEAF_PENDING, "resolve on an unfrozen map");
                    return l;
                }
                MapNode::Split { consumed, jbits, child_base } => {
                    node = child_base
                        + split_child(hashvalue, consumed as u32, jbits as u32);
                }
            }
        }
    }
}

/// Exact-arithmetic in-memory size model for building one file batch as a
/// shared table (the PLAN-BATCHES admission check; §7 buffer-memory
/// honesty). `file_bytes`/`tuples` are EXACT (directory + router counters —
/// never estimates; the agg/distinct duplicate-inflation class does not
/// exist on this path). The chunk term models the budget's CAPACITY charge:
/// each worker's last chunk may be near-empty, bounded by the leaf builds'
/// chunk cap.
pub fn estimate_batch_table_mem(
    file_bytes: u64,
    tuples: u64,
    workers: u64,
    chunk_cap_bytes: u64,
) -> u64 {
    // On file: 8B header + padded tuple. In memory: 24B header words +
    // padded tuple + 8B partition ref.
    let data = file_bytes
        .saturating_sub(8 * tuples)
        .saturating_add(32 * tuples);
    let buckets = 8 * tuples.max(1).next_power_of_two().clamp(1024, 1 << 31);
    data + workers.max(1) * chunk_cap_bytes + buckets
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mixr(x: u64) -> u64 {
        let mut x = x.wrapping_add(0x9e3779b97f4a7c15);
        x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
        x ^ (x >> 31)
    }

    #[test]
    fn record_roundtrip_varied_lengths() {
        let mut buf = Vec::new();
        let mut expect = Vec::new();
        for i in 0..500u64 {
            let len = (mixr(i) % 61) as usize; // includes 0
            let tuple: Vec<u8> = (0..len).map(|j| (i as u8).wrapping_add(j as u8)).collect();
            let h = mixr(i ^ 0xABCD) as u32;
            batch_record_push(&mut buf, h, &tuple);
            expect.push((h, tuple));
        }
        assert_eq!(buf.len() % 8, 0, "records are 8-aligned end to end");
        let mut it = BatchRecords::new(&buf);
        let mut got = Vec::new();
        while let Some((h, t)) = it.next_rec().unwrap() {
            got.push((h, t.to_vec()));
        }
        assert_eq!(got, expect);
    }

    #[test]
    fn tuple_slices_are_aligned_in_aligned_buffers() {
        let mut bytes = Vec::new();
        for i in 0..64u64 {
            batch_record_push(&mut bytes, i as u32, &vec![7u8; (i % 23) as usize]);
        }
        // Materialize into an 8-aligned buffer (the reader discipline).
        let mut words = vec![0u64; bytes.len() / 8];
        // SAFETY: plain byte copy into the owned word buffer.
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                words.as_mut_ptr().cast::<u8>(),
                bytes.len(),
            );
        }
        let view =
            unsafe { std::slice::from_raw_parts(words.as_ptr().cast::<u8>(), bytes.len()) };
        let mut it = BatchRecords::new(view);
        while let Some((_, t)) = it.next_rec().unwrap() {
            if !t.is_empty() {
                assert_eq!(t.as_ptr() as usize % 8, 0, "tuple image must be MAXALIGNed");
            }
        }
    }

    #[test]
    fn torn_records_fail_closed() {
        let mut buf = Vec::new();
        batch_record_push(&mut buf, 0xFEED, b"hello world!");
        // Torn tail: drop the last 8 bytes.
        let mut it = BatchRecords::new(&buf[..buf.len() - 8]);
        assert!(it.next_rec().is_err());
        // Short header.
        let mut it = BatchRecords::new(&buf[..4]);
        assert!(it.next_rec().is_err());
        // Corrupt length overrunning the extent.
        let mut bad = buf.clone();
        bad[4..8].copy_from_slice(&u32::MAX.to_ne_bytes());
        let mut it = BatchRecords::new(&bad);
        assert!(it.next_rec().is_err());
        // Clean empty input is a clean end.
        let mut it = BatchRecords::new(&[]);
        assert!(it.next_rec().unwrap().is_none());
    }

    #[test]
    fn level0_batches_partition_and_children_nest() {
        let n = 8u32;
        for i in 0..20_000u64 {
            let h = mixr(i) as u32;
            let b = batch_of(h, n.trailing_zeros());
            assert!(b < n);
            // Equal hashvalues route identically (trivially deterministic).
            assert_eq!(b, batch_of(h, n.trailing_zeros()));
            // A child slice never depends on bits its ancestors consumed:
            // two hashes in the same level-0 batch and same 2-bit child
            // agree on the top 3+2 remix bits.
            let c = split_child(h, 3, 2);
            assert!(c < 4);
            let reconstructed = (batch_mix(h) >> (64 - 5)) as u32;
            assert_eq!((b << 2) | c, reconstructed);
        }
    }

    #[test]
    fn leaf_map_resolves_through_splits() {
        let mut map = LeafMap::new(4);
        // Batch 0 = in-memory; batch 1 = leaf 0; batch 2 splits into 4
        // children (leaves 1..5); batch 3 = leaf 5's sibling then a deep
        // split on child 2.
        map.set_leaf(0, LEAF_INMEM);
        map.set_leaf(1, 0);
        let base = map.split_node(2, 2); // consumed=2, jbits=2
        for (i, leaf) in (0..4).zip(1u16..5) {
            if i == 2 {
                continue;
            }
            map.set_leaf(base + i, leaf);
        }
        // Deep split of child 2 (consumed = 2 + 2 = 4), 1 bit.
        let deep = map.split_child_node(base + 2, 4, 1);
        map.set_leaf(deep, 5);
        map.set_leaf(deep + 1, 6);
        map.set_leaf(3, 7);
        assert!(map.fully_resolved());

        for i in 0..50_000u64 {
            let h = mixr(i ^ 0x5EED) as u32;
            let leaf = map.resolve(h);
            let b = batch_of(h, 2);
            match b {
                0 => assert_eq!(leaf, LEAF_INMEM),
                1 => assert_eq!(leaf, 0),
                3 => assert_eq!(leaf, 7),
                2 => {
                    let c = split_child(h, 2, 2);
                    match c {
                        0 => assert_eq!(leaf, 1),
                        1 => assert_eq!(leaf, 2),
                        3 => assert_eq!(leaf, 4),
                        2 => {
                            let d = split_child(h, 4, 1);
                            assert_eq!(leaf, if d == 0 { 5 } else { 6 });
                        }
                        _ => unreachable!(),
                    }
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn unresolved_map_is_detected() {
        let mut map = LeafMap::new(2);
        map.set_leaf(0, LEAF_INMEM);
        assert!(!map.fully_resolved());
        map.set_leaf(1, 0);
        assert!(map.fully_resolved());
    }

    #[test]
    fn estimate_model_monotone_and_exact_terms() {
        // 100 tuples of 40 payload bytes: file = 100 × (8 + 40) = 4800.
        let file = 4800u64;
        let est = estimate_batch_table_mem(file, 100, 4, 1 << 20);
        // data = 4800 - 800 + 3200 = 7200; buckets = 8 × 1024; chunks 4MB.
        assert_eq!(est, 7200 + 4 * (1 << 20) + 8 * 1024);
        assert!(estimate_batch_table_mem(file * 2, 200, 4, 1 << 20) > est);
    }
}
