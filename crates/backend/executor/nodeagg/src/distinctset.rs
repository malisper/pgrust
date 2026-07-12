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
use ::mcx::Mcx;
use ::sort_storage::{LogicalTapeSet, TapeIdx};
use ::types_error::PgResult;
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
pub(crate) struct DistinctSet<'mcx> {
    /// Open-addressing table: slot -> entry index + 1; 0 = empty. Pow2 len.
    table: Vec<u32>,
    ints: Vec<i64>,
    blob: Vec<u8>,
    spans: Vec<BytesSpan>,
    pub(crate) seen_null: bool,
    /// v2 big-NDV spill (hash-partitioned flush runs onto logical tapes);
    /// `Some` once the first work_mem crossing chose the spill path. See the
    /// `SpillState` doc for the design + memory-bound argument.
    spill: Option<SpillState<'mcx>>,
}

// ===========================================================================
// v2 set spill — hash-partitioned flush runs (uniqExact big-NDV survival).
//
// Design (charter "SET SPILL" lever, radix-partitioned variant): the key
// space is partitioned by the TOP bits of the full-avalanche key hash
// (`spill_part`) into `nparts` DISJOINT partitions, one logical tape each
// (logtape.c's serial tape set: one temp file, per-tape block chains,
// blocks recycled through the freelist). Whenever the in-memory set crosses
// its work_mem budget, `spill_flush` appends every held value to its
// partition's tape and clears the set (capacities retained; a fill-level
// trigger captured at the first flush keeps the crossing check meaningful
// afterwards — capacity-based `mem_bytes` stays above budget once grown).
// The in-memory set keeps deduplicating WITHIN each flush epoch; the same
// value seen in two epochs is written twice and re-deduplicated at
// finalize.
//
// Finalize (`spill_load_partition` + lib.rs's replay): partitions are
// disjoint, so the group's distinct multiset is the disjoint union of the
// partitions' distinct sets — each partition is loaded alone into the
// (cleared) in-memory set, deduplicated there, replayed through the real
// transfn, and dropped before the next partition loads. No cross-partition
// merge exists by construction. Expected per-partition load is
// NDV/nparts; a skewed/huge partition that would itself cross the budget
// stops loading (`Ok(false)`) and the caller finishes THAT partition on a
// work_mem-bounded tuplesort (`spill_read_*` streams the tape's remaining
// raw values) — the C sort path's own spill machinery, per partition, so
// memory stays bounded for any NDV.
//
// Memory honesty: in-memory set ≤ budget + one insert; tape write buffers
// are BLCKSZ per partition, lazily allocated, and `spill_parts_for_budget`
// sizes `nparts` so they stay a small fraction of the budget (spilling is
// refused entirely below SPILL_MIN_BUDGET — the caller keeps the v1
// degrade-to-tuplesort path there, and for whatever else v2 refuses).
// Per-partition metadata is O(nparts), not O(runs): tapes are append
// streams, so flush count leaves no trace but the data itself.
//
// Value identity: exactly the v1 argument — the spill changes only the
// transfn REPLAY ORDER over the identical distinct-value multiset (dedup is
// exact: partition-local sets use the same representational-equality keys,
// and partitions are disjoint), and the admitted transitions are
// order-insensitive. NULLs never touch the tapes: `seen_null` survives
// flushes in memory and replays once, exactly as v1.
// ===========================================================================

/// Number of key-hash TOP bits consumed by partitioning at 32 partitions.
const SPILL_MAX_PARTS: usize = 32;
/// Budgets below this keep the v1 degrade path: nparts*BLCKSZ tape write
/// buffers must stay a small fraction of the budget for the spill to be
/// memory-honest.
pub(crate) const SPILL_MIN_BUDGET: usize = 128 * 1024;
const BLCKSZ: usize = 8192;

/// Partition count for `budget`: pow2, tape write buffers (nparts * BLCKSZ)
/// capped at ~1/4 of the budget, at most SPILL_MAX_PARTS.
fn spill_parts_for_budget(budget: usize) -> usize {
    let cap = (budget / (4 * BLCKSZ)).max(4);
    let mut p = 4usize;
    while p * 2 <= cap && p * 2 <= SPILL_MAX_PARTS {
        p *= 2;
    }
    p
}

struct SpillState<'mcx> {
    tapes_set: LogicalTapeSet<'mcx>,
    /// One append tape per partition; index = partition.
    tapes: Vec<TapeIdx>,
    /// In-memory fill levels captured at the first flush: once capacities
    /// have grown past the budget, `mem_bytes` can no longer signal the next
    /// crossing, so `over_budget` compares fill against these instead.
    flush_len: usize,
    flush_blob: usize,
    /// Finalize state: tapes rewound for reading (write side closed).
    reading: bool,
}

/// Partition of a full-avalanche 64-bit key hash: top log2(nparts) bits
/// (the probe uses the LOW bits via the pow2 table mask, so partition and
/// probe bits are independent).
#[inline]
fn spill_part(h: u64, nparts: usize) -> usize {
    ((h >> 32) as usize) & (nparts - 1)
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

impl<'mcx> DistinctSet<'mcx> {
    pub(crate) fn new() -> Self {
        DistinctSet {
            table: Vec::new(),
            ints: Vec::new(),
            blob: Vec::new(),
            spans: Vec::new(),
            seen_null: false,
            spill: None,
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
    /// group refills a same-shaped set). Any spill state is released too —
    /// the finalize consumed it (or a rescan is abandoning it: the temp file
    /// closes; on close failure end-of-xact fd cleanup owns it, as every
    /// BufFile user).
    pub(crate) fn clear(&mut self) {
        self.reset_values();
        self.seen_null = false;
        if let Some(sp) = self.spill.take() {
            let _ = sp.tapes_set.close();
        }
    }

    /// Flush-time reset: values only — `seen_null` (never spilled) and the
    /// spill state survive; capacities are retained for the next epoch.
    fn reset_values(&mut self) {
        self.table.iter_mut().for_each(|s| *s = 0);
        self.ints.clear();
        self.blob.clear();
        self.spans.clear();
    }

    /// Degrade-time reset: give the memory back (the tuplesort owns the
    /// group's values now).
    pub(crate) fn clear_shrink(&mut self) {
        self.clear();
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
    #[inline]
    pub(crate) fn insert_i64(&mut self, k: i64) {
        self.insert_i64_hashed(k, mix64(k as u64));
    }

    /// Staged batch insert (the lane drives' direct-key feed): pass 1 mixes
    /// every hash in one tight loop over the staged key lane, pass 2 probes
    /// in row order with the precomputed hash. Element-for-element identical
    /// to `insert_i64` in the same order.
    pub(crate) fn insert_i64_batch(&mut self, keys: &[i64], hashes: &mut Vec<u64>) {
        hashes.clear();
        hashes.extend(keys.iter().map(|&k| mix64(k as u64)));
        for (&k, &h) in keys.iter().zip(hashes.iter()) {
            self.insert_i64_hashed(k, h);
        }
    }

    #[inline]
    fn insert_i64_hashed(&mut self, k: i64, h: u64) {
        let mask = self.probe_ready();
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

    // ------------------------------------------------------------------
    // v2 spill (section doc above `SpillState`).
    // ------------------------------------------------------------------

    #[inline]
    pub(crate) fn spilled(&self) -> bool {
        self.spill.is_some()
    }

    /// The budget-crossing check. Pre-spill it is the v1 capacity check;
    /// once spilled, capacities stay above the budget forever, so the
    /// fill levels captured at the first flush signal the next epoch's
    /// crossing instead.
    #[inline]
    pub(crate) fn over_budget(&self, budget: usize) -> bool {
        match &self.spill {
            None => self.mem_bytes() > budget,
            Some(sp) => {
                self.len() >= sp.flush_len
                    || (sp.flush_blob > 0 && self.blob.len() >= sp.flush_blob)
            }
        }
    }

    /// Append every held value to its partition's tape and clear the values
    /// (capacities and `seen_null` retained). First call creates the tape
    /// set; `budget` fixes the partition count then.
    pub(crate) fn spill_flush(
        &mut self,
        kind: DistinctKeyKind,
        budget: usize,
        mcx: Mcx<'mcx>,
    ) -> PgResult<()> {
        if self.spill.is_none() {
            let mut tapes_set = LogicalTapeSet::create(mcx, false)?;
            let nparts = spill_parts_for_budget(budget);
            let tapes = (0..nparts).map(|_| tapes_set.create_tape()).collect();
            self.spill = Some(SpillState {
                tapes_set,
                tapes,
                flush_len: self.len().max(1),
                flush_blob: self.blob.len(),
                reading: false,
            });
        }
        {
            let DistinctSet { spill, ints, spans, blob, .. } = self;
            let sp = spill.as_mut().expect("armed above");
            debug_assert!(!sp.reading);
            let nparts = sp.tapes.len();
            match kind {
                DistinctKeyKind::Int16 | DistinctKeyKind::Int32 | DistinctKeyKind::Int64 => {
                    for &k in ints.iter() {
                        let p = spill_part(mix64(k as u64), nparts);
                        sp.tapes_set.write(sp.tapes[p], &k.to_ne_bytes())?;
                    }
                }
                DistinctKeyKind::Bytes => {
                    for s in spans.iter() {
                        // Record = u32 content length + content bytes; the
                        // partition reuses the stored content hash (any
                        // deterministic function of the content works).
                        let p = spill_part(mix64(s.hash as u64), nparts);
                        sp.tapes_set.write(sp.tapes[p], &s.len.to_ne_bytes())?;
                        let at = s.off as usize + varatt::VARHDRSZ;
                        sp.tapes_set.write(sp.tapes[p], &blob[at..at + s.len as usize])?;
                    }
                }
            }
        }
        self.reset_values();
        Ok(())
    }

    #[inline]
    pub(crate) fn spill_nparts(&self) -> usize {
        self.spill.as_ref().map_or(0, |sp| sp.tapes.len())
    }

    /// Finalize step 1: flush the residual epoch (uniform per-partition
    /// handling) and rewind every tape for reading.
    pub(crate) fn spill_finish_writes(
        &mut self,
        kind: DistinctKeyKind,
        budget: usize,
        mcx: Mcx<'mcx>,
    ) -> PgResult<()> {
        debug_assert!(self.spilled());
        self.spill_flush(kind, budget, mcx)?;
        // Drop the build-phase capacities (they crossed the budget by
        // definition): the per-partition loads regrow to partition size, and
        // `mem_bytes` — capacity-based — must meter THAT, not the build peak.
        self.table = Vec::new();
        self.ints = Vec::new();
        self.blob = Vec::new();
        self.spans = Vec::new();
        let sp = self.spill.as_mut().expect("spilled");
        for i in 0..sp.tapes.len() {
            sp.tapes_set.rewind_for_read(sp.tapes[i], BLCKSZ)?;
        }
        sp.reading = true;
        Ok(())
    }

    /// Finalize step 2, per partition: load partition `p`'s values into the
    /// cleared set (exact dedup — flush epochs may have written a value more
    /// than once), stopping if the load itself crosses `budget`.
    /// `Ok(true)` = complete: the set holds exactly partition `p`'s distinct
    /// values (tape closed). `Ok(false)` = the partition alone exceeds the
    /// budget: the set holds a deduplicated prefix and the caller must
    /// stream the remainder through `spill_read_ints`/`spill_read_bytes`
    /// into a work_mem-bounded tuplesort.
    pub(crate) fn spill_load_partition(
        &mut self,
        kind: DistinctKeyKind,
        p: usize,
        budget: usize,
    ) -> PgResult<bool> {
        debug_assert!(self.spill.as_ref().is_some_and(|sp| sp.reading));
        self.reset_values();
        match kind {
            DistinctKeyKind::Int16 | DistinctKeyKind::Int32 | DistinctKeyKind::Int64 => {
                let mut buf = [0u8; 4096];
                loop {
                    let n = {
                        let sp = self.spill.as_mut().expect("spilled");
                        sp.tapes_set.read(sp.tapes[p], &mut buf)?
                    };
                    if n == 0 {
                        break;
                    }
                    debug_assert_eq!(n % 8, 0, "int spill tape holds whole i64 records");
                    for c in buf[..n].chunks_exact(8) {
                        self.insert_i64(i64::from_ne_bytes(c.try_into().unwrap()));
                    }
                    if self.mem_bytes() > budget {
                        return Ok(false);
                    }
                }
            }
            DistinctKeyKind::Bytes => {
                let mut rec: Vec<u8> = Vec::new();
                loop {
                    let more = {
                        let sp = self.spill.as_mut().expect("spilled");
                        read_bytes_record(sp, p, &mut rec)?
                    };
                    if !more {
                        break;
                    }
                    self.insert_bytes(&rec);
                    if self.mem_bytes() > budget {
                        return Ok(false);
                    }
                }
            }
        }
        let sp = self.spill.as_mut().expect("spilled");
        sp.tapes_set.close_tape(sp.tapes[p]);
        Ok(true)
    }

    /// Stream raw i64 values remaining on partition `p`'s tape after a
    /// partial `spill_load_partition` (values may repeat across epochs; the
    /// consumer dedups). Appends up to one chunk to `out`; `Ok(false)` = tape
    /// exhausted and closed.
    pub(crate) fn spill_read_ints(&mut self, p: usize, out: &mut Vec<i64>) -> PgResult<bool> {
        let sp = self.spill.as_mut().expect("spilled");
        let mut buf = [0u8; 4096];
        let n = sp.tapes_set.read(sp.tapes[p], &mut buf)?;
        if n == 0 {
            sp.tapes_set.close_tape(sp.tapes[p]);
            return Ok(false);
        }
        debug_assert_eq!(n % 8, 0, "int spill tape holds whole i64 records");
        out.extend(buf[..n].chunks_exact(8).map(|c| i64::from_ne_bytes(c.try_into().unwrap())));
        Ok(true)
    }

    /// One raw bytes record remaining on partition `p`'s tape after a
    /// partial load; `Ok(false)` = tape exhausted and closed.
    pub(crate) fn spill_read_bytes(&mut self, p: usize, out: &mut Vec<u8>) -> PgResult<bool> {
        let sp = self.spill.as_mut().expect("spilled");
        let more = read_bytes_record(sp, p, out)?;
        if !more {
            sp.tapes_set.close_tape(sp.tapes[p]);
        }
        Ok(more)
    }

    /// Finalize complete: release the spill (temp file closes).
    pub(crate) fn spill_end(&mut self) -> PgResult<()> {
        if let Some(sp) = self.spill.take() {
            sp.tapes_set.close()?;
        }
        Ok(())
    }
}

/// Build a canonical 4B-header varlena image of `content` into `img` (u32
/// backing — text's 4-byte typalign) and return its by-ref datum (live until
/// `img` is next touched).
pub(crate) fn varlena_image(content: &[u8], img: &mut Vec<u32>) -> Datum {
    let total = varatt::VARHDRSZ + content.len();
    img.clear();
    img.resize(total.div_ceil(4), 0);
    img[0] = varatt::set_varsize_4b_word(total as u32);
    // SAFETY: img holds ceil(total/4) u32s ≥ total bytes past the header.
    unsafe {
        core::ptr::copy_nonoverlapping(
            content.as_ptr(),
            (img.as_mut_ptr() as *mut u8).add(varatt::VARHDRSZ),
            content.len(),
        );
    }
    Datum::from_usize(img.as_ptr() as usize)
}

/// Read one (u32 len, content) record off partition `p`'s tape into `out`
/// (cleared); false = EOF.
fn read_bytes_record(sp: &mut SpillState<'_>, p: usize, out: &mut Vec<u8>) -> PgResult<bool> {
    let mut lenbuf = [0u8; 4];
    let n = sp.tapes_set.read(sp.tapes[p], &mut lenbuf)?;
    if n == 0 {
        return Ok(false);
    }
    debug_assert_eq!(n, 4, "bytes spill tape holds whole records");
    let len = u32::from_ne_bytes(lenbuf) as usize;
    out.clear();
    out.resize(len, 0);
    if len > 0 {
        let got = sp.tapes_set.read(sp.tapes[p], out)?;
        debug_assert_eq!(got, len, "bytes spill tape holds whole records");
    }
    Ok(true)
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
