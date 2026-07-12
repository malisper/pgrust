//! Lane-v2 exact-DISTINCT set state — the uniqExact analog (cbstore-v2 plan
//! §2.3; both executor catalogs' set-state designs).
//!
//! One `DistinctSet` replaces the per-group TUPLESORT a non-presorted
//! DISTINCT aggregate otherwise runs (C nodeAgg's sortstates +
//! process_ordered_aggregate_single): the transition phase becomes set-insert
//! and the group finalize replays each distinct value once through the real
//! transfn. Value-identity with the C sort path holds because admission
//! (lib.rs `distinct_set_kind`) restricts to transitions that are
//! order-insensitive over a distinct-value multiset (count/sum/avg over ints,
//! count over deterministic-collation text) — the set changes only the
//! REPLAY ORDER, which those transfns cannot observe.
//!
//! Equality/hash pairing (charter: PG's own equality, equal-values-must-
//! hash-equal): admission proves the aggregate's DISTINCT equality operator
//! is *representational* equality —
//!   * int2/int4/int8: `int2eq`/`int4eq`/`int8eq` are value equality on the
//!     sign-extended word; the key stored here IS that sign-extended i64
//!     (`Datum::as_i16/as_i32/as_i64`), so set equality == PG equality and
//!     ANY deterministic hash of the key satisfies equal-hashes-equal.
//!   * text/varchar under a DETERMINISTIC collation: `texteq` is
//!     length+memcmp of the detoasted content bytes (varlena.rs `texteq`,
//!     the deterministic arm); the key here is exactly those content bytes.
//!     Nondeterministic collations (equal-but-byte-different) REFUSE at
//!     admission.
//! No numeric-style class types are admitted (numeric 1.0 == 1.00 would need
//! the type's own hash function); that is why the hash below can be a plain
//! mixer rather than the fmgr hash proc.
//!
//! The set is deliberately minimal open addressing (linear probe, pow2
//! table, entry-index slots): the C-ported tuplehash carries MinimalTuple +
//! per-entry context machinery this state does not need. A compact-set /
//! ported-tuplehash A/B is the Stage-2.2 companion measurement.
//!
//! Merge-shaped by design (Stage-4 payoff): the state is a plain value set —
//! set-union of two `DistinctSet`s over the same key kind is the natural
//! partial-aggregate merge. No parallel plumbing exists yet; nothing here
//! assumes single-threadedness except &mut.

use ::datum::Datum;
use ::types_tuple::varatt;

/// Admitted DISTINCT-argument representations (lib.rs `distinct_set_kind`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DistinctKeyKind {
    /// int2 argument; key = sign-extended i64 (int2eq semantics).
    Int16,
    /// int4 argument; key = sign-extended i64 (int4eq semantics).
    Int32,
    /// int8 argument; key = i64 (int8eq semantics).
    Int64,
    /// text/varchar under a deterministic collation; key = detoasted content
    /// bytes (texteq's deterministic length+memcmp arm).
    Bytes,
}

/// A stored text value: a canonical 4-byte-header varlena image in `blob`
/// (replay hands its pointer to the transfn), keyed on the content bytes.
struct BytesSpan {
    /// Offset of the varlena IMAGE (header included) in `blob`; 8-aligned.
    off: u32,
    /// Content length (bytes after the 4-byte header).
    len: u32,
    /// Saved content hash (rehash + probe prefilter).
    hash: u32,
}

/// Exact-distinct hash set over one admitted key kind. Either `ints` or
/// (`blob`+`spans`) is populated, never both (the kind is fixed per
/// pertrans). `seen_null` stands in for the at-most-one NULL the C sort path
/// dedups to (two NULLs are "equal" for DISTINCT — nodeAgg.c
/// process_ordered_aggregate_single's `oldIsNull && *isNull` arm); the
/// replay passes it through the same transfn call C would.
pub(crate) struct DistinctSet {
    /// Open-addressing table: slot -> entry index + 1; 0 = empty. Pow2 len.
    table: Vec<u32>,
    ints: Vec<i64>,
    blob: Vec<u8>,
    spans: Vec<BytesSpan>,
    pub(crate) seen_null: bool,
}

/// splitmix64 finalizer — a full-avalanche mixer for the i64 keys. NOT PG's
/// hash function: legal because admitted equality is representational (see
/// module doc), so any deterministic hash of the canonical key satisfies
/// equal-values-hash-equal.
#[inline]
fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

const INIT_TABLE: usize = 64;

impl DistinctSet {
    pub(crate) fn new() -> Self {
        DistinctSet {
            table: Vec::new(),
            ints: Vec::new(),
            blob: Vec::new(),
            spans: Vec::new(),
            seen_null: false,
        }
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.ints.len() + self.spans.len()
    }

    /// Bytes the set holds (capacities — actual allocation, the conservative
    /// figure the work_mem budget check wants).
    pub(crate) fn mem_bytes(&self) -> usize {
        self.table.capacity() * core::mem::size_of::<u32>()
            + self.ints.capacity() * core::mem::size_of::<i64>()
            + self.blob.capacity()
            + self.spans.capacity() * core::mem::size_of::<BytesSpan>()
    }

    /// Group-boundary reset: drop the values, keep the allocations (the next
    /// group refills a same-shaped set).
    pub(crate) fn clear(&mut self) {
        self.table.iter_mut().for_each(|s| *s = 0);
        self.ints.clear();
        self.blob.clear();
        self.spans.clear();
        self.seen_null = false;
    }

    /// Degrade-time reset: give the memory back (the tuplesort owns the
    /// group's values now).
    pub(crate) fn clear_shrink(&mut self) {
        *self = DistinctSet::new();
    }

    /// Grow-if-needed, then return the probe mask. 7/8 load factor.
    #[inline]
    fn probe_ready(&mut self) -> usize {
        let len = self.len();
        if self.table.is_empty() {
            self.table.resize(INIT_TABLE, 0);
        } else if (len + 1) * 8 > self.table.len() * 7 {
            self.grow();
        }
        self.table.len() - 1
    }

    #[cold]
    #[inline(never)]
    fn grow(&mut self) {
        let new_len = self.table.len() * 2;
        let mask = new_len - 1;
        let mut table = vec![0u32; new_len];
        let rehash = |table: &mut [u32], h: u64, e: u32| {
            let mut slot = (h as usize) & mask;
            while table[slot] != 0 {
                slot = (slot + 1) & mask;
            }
            table[slot] = e;
        };
        for (i, &k) in self.ints.iter().enumerate() {
            rehash(&mut table, mix64(k as u64), (i + 1) as u32);
        }
        for (i, sp) in self.spans.iter().enumerate() {
            rehash(&mut table, mix64(sp.hash as u64), (i + 1) as u32);
        }
        self.table = table;
    }

    /// Insert a sign-extended integer key (no-op if present).
    pub(crate) fn insert_i64(&mut self, k: i64) {
        let mask = self.probe_ready();
        let h = mix64(k as u64);
        let mut slot = (h as usize) & mask;
        loop {
            match self.table[slot] {
                0 => {
                    self.ints.push(k);
                    self.table[slot] = self.ints.len() as u32;
                    return;
                }
                e => {
                    if self.ints[(e - 1) as usize] == k {
                        return;
                    }
                    slot = (slot + 1) & mask;
                }
            }
        }
    }

    /// Insert detoasted text CONTENT bytes (no-op if present). Stores a
    /// canonical 4B-header varlena image so replay can hand the transfn a
    /// live datum pointer.
    pub(crate) fn insert_bytes(&mut self, content: &[u8]) {
        let mask = self.probe_ready();
        let hash = ::hashfn::hash_bytes(content);
        let h = mix64(hash as u64);
        let mut slot = (h as usize) & mask;
        loop {
            match self.table[slot] {
                0 => {
                    // 8-align the image (palloc alignment; varlena header
                    // reads stay in-bounds and aligned).
                    let pad = (8 - (self.blob.len() & 7)) & 7;
                    self.blob.resize(self.blob.len() + pad, 0);
                    let off = self.blob.len();
                    let word = varatt::set_varsize_4b_word(
                        (content.len() + varatt::VARHDRSZ) as u32,
                    );
                    self.blob.extend_from_slice(&word.to_ne_bytes());
                    self.blob.extend_from_slice(content);
                    self.spans.push(BytesSpan {
                        off: off as u32,
                        len: content.len() as u32,
                        hash,
                    });
                    self.table[slot] = self.spans.len() as u32;
                    return;
                }
                e => {
                    let sp = &self.spans[(e - 1) as usize];
                    if sp.hash == hash
                        && sp.len as usize == content.len()
                        && &self.blob[sp.off as usize + varatt::VARHDRSZ
                            ..sp.off as usize + varatt::VARHDRSZ + sp.len as usize]
                            == content
                    {
                        return;
                    }
                    slot = (slot + 1) & mask;
                }
            }
        }
    }

    /// The distinct integer keys, insertion order (order is replay-invisible
    /// — module doc).
    #[inline]
    pub(crate) fn ints(&self) -> &[i64] {
        &self.ints
    }

    #[inline]
    pub(crate) fn n_bytes(&self) -> usize {
        self.spans.len()
    }

    /// Datum for stored text value `i`: a pointer to the canonical varlena
    /// image inside `blob`. Live until the next `insert_bytes`/`clear`.
    #[inline]
    pub(crate) fn bytes_datum(&self, i: usize) -> Datum {
        Datum::from_usize(self.blob[self.spans[i].off as usize..].as_ptr() as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn int_dedup_and_growth() {
        let mut s = DistinctSet::new();
        for round in 0..3 {
            for i in 0..10_000i64 {
                s.insert_i64(i * 7 - 5_000);
            }
            assert_eq!(s.len(), 10_000, "round {round}");
        }
        s.insert_i64(i64::MIN);
        s.insert_i64(i64::MAX);
        s.insert_i64(0);
        assert_eq!(s.len(), 10_003);
        s.clear();
        assert_eq!(s.len(), 0);
        assert!(!s.seen_null);
        s.insert_i64(42);
        assert_eq!(s.ints(), &[42]);
    }

    #[test]
    fn bytes_dedup_and_images() {
        let mut s = DistinctSet::new();
        for round in 0..2 {
            for i in 0..1_000u32 {
                s.insert_bytes(format!("value-{i}").as_bytes());
            }
            assert_eq!(s.len(), 1_000, "round {round}");
        }
        s.insert_bytes(b"");
        assert_eq!(s.len(), 1_001);
        // Every stored image is a valid 4B varlena whose content round-trips.
        for i in 0..s.n_bytes() {
            let d = s.bytes_datum(i);
            let p = d.as_usize() as *const u8;
            // SAFETY: bytes_datum points at a canonical in-blob image.
            unsafe {
                assert!(!varatt::varatt_is_1b(p));
                let n = varatt::varsize_4b(p) - varatt::VARHDRSZ;
                let content = core::slice::from_raw_parts(p.add(varatt::VARHDRSZ), n);
                if n == 0 {
                    assert_eq!(content, b"");
                } else {
                    assert!(content.starts_with(b"value-"));
                }
            }
        }
        assert!(s.mem_bytes() > 1_000 * 8);
    }

    #[test]
    fn hash_collision_still_compares_bytes() {
        // Same length, different content: even if the 32-bit hashes ever
        // collided, the memcmp arm keeps them distinct.
        let mut s = DistinctSet::new();
        s.insert_bytes(b"abcd");
        s.insert_bytes(b"abce");
        s.insert_bytes(b"abcd");
        assert_eq!(s.len(), 2);
    }
}
