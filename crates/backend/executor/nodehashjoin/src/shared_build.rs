//! M3 shared-build hash join — the two-phase (Leis-style) build core.
//!
//! Design authority: docs/design/m3-joins.md §3 (JoinBuildLocal:
//! materialize + count, thread-local), §4 (partitioned single-writer
//! combine, run-ordered deterministic chains, 16-bit tag words), §5
//! (match flags for the right-fill family). This module is PURE data
//! structure + algorithm: no executor, no runtime-crate dependency —
//! the ParallelSink impl and engagement wiring arrive in inc-2
//! (execmain/lanev2/runtime_hashjoin.rs), mirroring the m2-agg-sink
//! split (nodeagg/src/sink.rs core vs execmain wiring).
//!
//! # Determinism (the §4 load-bearing claim)
//!
//! The serial build inserts inner rows in scan order, each at its
//! bucket's CHAIN HEAD. Here every accepted morsel is recorded as a RUN
//! (`begin_run(range_start)` … `end_run()`); morsel ranges are disjoint,
//! so `range_start` totally orders the runs regardless of which worker
//! claimed what, in what order, at what adaptive sizing. Combine walks
//! runs ascending by `range_start`, within a run in materialization
//! (= scan) order, head-inserting — reproducing the serial chain
//! byte-for-byte for every bucket. The property tests below drive
//! adversarial claim schedules (single-worker-takes-all, maximal
//! interleave, ramp/photo-finish sizing, non-ascending per-worker claim
//! order) against a serial reference build.
//!
//! # Concurrency contract (enforced by the caller, asserted here)
//!
//! - A `JoinBuildLocal` is single-threaded (one worker's sink Local).
//! - `SealedBuild::combine_partition(part)` is called EXACTLY ONCE per
//!   partition (the ParallelSink combine contract); a partition's bucket
//!   range and its tuples' `next` words have a single writer. Stores are
//!   relaxed atomics; cross-task visibility is the runtime's task-set
//!   completion barrier (deps DAG + last-worker-out, Loom-verified in
//!   the runtime crate).
//! - After `finish()`, the table is frozen: probe reads everything;
//!   the ONLY writes are the right-fill match flags (idempotent
//!   monotonic atomic, §5), read by the FILL phase after the probe
//!   set's completion barrier.

use std::cell::UnsafeCell;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// Combine partition count (the ParallelSink partition space). Top 8
/// bits of the hashvalue select the partition; the bucket index keeps
/// the partition in its top bits so each partition owns a contiguous,
/// exclusive bucket range (§4 single-writer argument).
pub const PARTITIONS: usize = 256;

const MIN_NBUCKETS: u64 = 1024;
const MAX_NBUCKETS: u64 = 1 << 31;

/// Packed tuple reference: ordinal(8) | chunk(8) | word-offset(32) = 48
/// bits. Stored shifted/+1 so 0 can mean "empty"/"end of chain".
const REF_OFFSET_BITS: u32 = 32;
const REF_CHUNK_BITS: u32 = 8;

/// Tuple header: 3 words before the payload.
///   W0: next — packed (ref+1) of the next chain tuple; 0 = end.
///   W1: payload length in bytes (high 32) | hashvalue (low 32).
///   W2: match flag (right-fill family), 0/1.
const HDR_WORDS: usize = 3;

/// Chunk sizing: bump-allocated word buffers, doubling 64KB → 16MB.
const CHUNK_MIN_WORDS: usize = 8 << 10; // 64KB
const CHUNK_MAX_WORDS: usize = 2 << 20; // 16MB
const MAX_CHUNKS_PER_LOCAL: usize = 1 << REF_CHUNK_BITS;

#[inline]
pub fn partition_of(hashvalue: u32) -> usize {
    (hashvalue >> 24) as usize
}

#[inline]
fn tag_bit(hashvalue: u32) -> u64 {
    1u64 << ((hashvalue >> 16) & 15)
}

#[inline]
fn bucket_of(hashvalue: u32, log2_nbuckets: u32) -> usize {
    let low = log2_nbuckets - 8;
    let within = (hashvalue as u64) & ((1u64 << low) - 1);
    (((hashvalue >> 24) as usize) << low) | within as usize
}

#[inline]
fn pack_ref(ordinal: u8, chunk: usize, off_words: usize) -> u64 {
    debug_assert!(chunk < MAX_CHUNKS_PER_LOCAL);
    debug_assert!(off_words <= u32::MAX as usize);
    ((ordinal as u64) << (REF_CHUNK_BITS + REF_OFFSET_BITS))
        | ((chunk as u64) << REF_OFFSET_BITS)
        | off_words as u64
}

#[inline]
fn unpack_ref(r: u64) -> (usize, usize, usize) {
    (
        (r >> (REF_CHUNK_BITS + REF_OFFSET_BITS)) as usize,
        ((r >> REF_OFFSET_BITS) & ((1 << REF_CHUNK_BITS) - 1)) as usize,
        (r & (u32::MAX as u64)) as usize,
    )
}

// ---------------------------------------------------------------------------
// Budget (§6): shared byte accounting against the C combined envelope.
// Admission arithmetic (exec_choose_hash_table_size_full) lives with the
// inc-2 wiring; this is the runtime enforcement half.
// ---------------------------------------------------------------------------

pub struct JoinBudget {
    limit: usize,
    used: AtomicUsize,
}

impl JoinBudget {
    pub fn new(limit: usize) -> Arc<JoinBudget> {
        Arc::new(JoinBudget { limit, used: AtomicUsize::new(0) })
    }

    pub fn unlimited() -> Arc<JoinBudget> {
        JoinBudget::new(usize::MAX)
    }

    /// Charge `n` bytes; false ⇔ the shared envelope is crossed (the
    /// caller records a refusal and aborts the RG — R5 whole-attempt
    /// rerun; the charge is deliberately left in place, the RG dies).
    fn try_charge(&self, n: usize) -> bool {
        let prev = self.used.fetch_add(n, Ordering::Relaxed);
        prev.saturating_add(n) <= self.limit
    }

    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }
}

/// The build crossed the memory envelope (§6): refusal, not an error
/// path — the engagement aborts to the serial arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetExceeded;

// ---------------------------------------------------------------------------
// Chunk: word-granular bump storage with post-freeze interior mutability
// for exactly two header words (next: combine single-writer; match flag:
// probe/fill atomics).
// ---------------------------------------------------------------------------

struct Chunk {
    words: Box<[UnsafeCell<u64>]>,
    used: usize,
}

// SAFETY: cross-thread access follows the module contract: payload and
// W1 words are written only by the owning Local before seal and only
// read after; W0 (next) has a single writer (the owning partition's
// combine task) ordered before all readers by the task-set barrier;
// W2 (match) is accessed only through &AtomicU64 views.
unsafe impl Send for Chunk {}
unsafe impl Sync for Chunk {}

impl Chunk {
    fn new(words: usize) -> Chunk {
        let v: Vec<UnsafeCell<u64>> = (0..words).map(|_| UnsafeCell::new(0)).collect();
        Chunk { words: v.into_boxed_slice(), used: 0 }
    }

    #[inline]
    fn capacity_bytes(&self) -> usize {
        self.words.len() * 8
    }

    #[inline]
    fn remaining(&self) -> usize {
        self.words.len() - self.used
    }

    /// SAFETY: caller respects the single-writer/atomic-view contract.
    #[inline]
    unsafe fn word_mut(&self, i: usize) -> *mut u64 {
        self.words[i].get()
    }

    #[inline]
    fn atomic(&self, i: usize) -> &AtomicU64 {
        // SAFETY: AtomicU64 and UnsafeCell<u64> are both 8-byte plain
        // wrappers over u64 with the same layout; the boxed slice keeps
        // the word alive and aligned.
        unsafe { &*(self.words[i].get() as *const AtomicU64) }
    }

    #[inline]
    fn read(&self, i: usize) -> u64 {
        // Post-freeze plain read of a word no longer written (W1,
        // payload) — routed through the cell pointer.
        unsafe { *self.words[i].get() }
    }
}

// ---------------------------------------------------------------------------
// JoinBuildLocal (§3): the worker's sink Local.
// ---------------------------------------------------------------------------

struct RunHeader {
    range_start: u64,
    /// Cumulative per-partition end indices into this Local's
    /// `part_refs` vectors at the run's close. Run r's partition-p slice
    /// is `part_refs[p][runs[r-1].ends[p] .. runs[r].ends[p]]`.
    ends: Box<[u32]>,
}

pub struct JoinBuildLocal {
    ordinal: u8,
    chunks: Vec<Chunk>,
    /// Per-partition tuple refs in materialization (= scan) order.
    part_refs: Vec<Vec<u64>>,
    runs: Vec<RunHeader>,
    in_run: bool,
    tuples: u64,
    budget: Arc<JoinBudget>,
}

impl JoinBuildLocal {
    /// `ordinal` = the sink worker index (worker-indexed Local slots,
    /// R3 pinned regime); must be < 256 (asserted — the pin-board lane
    /// space is 16+64, comfortably inside).
    pub fn new(ordinal: usize, budget: Arc<JoinBudget>) -> JoinBuildLocal {
        assert!(ordinal < 256, "join build Local ordinal {ordinal} out of ref range");
        JoinBuildLocal {
            ordinal: ordinal as u8,
            chunks: Vec::new(),
            part_refs: (0..PARTITIONS).map(|_| Vec::new()).collect(),
            runs: Vec::new(),
            in_run: false,
            tuples: 0,
            budget,
        }
    }

    /// Open the run for one accepted morsel. `range_start` = the claimed
    /// range's first granule — the determinism key: ranges are disjoint,
    /// so starts totally order the runs globally.
    pub fn begin_run(&mut self, range_start: u64) {
        assert!(!self.in_run, "begin_run inside an open run");
        self.runs.push(RunHeader {
            range_start,
            ends: vec![0u32; PARTITIONS].into_boxed_slice(),
        });
        self.in_run = true;
    }

    /// Close the current run: snapshot per-partition cumulative ends.
    pub fn end_run(&mut self) {
        assert!(self.in_run, "end_run without an open run");
        let ends = &mut self.runs.last_mut().expect("open run").ends;
        for (p, refs) in self.part_refs.iter().enumerate() {
            ends[p] = u32::try_from(refs.len()).expect("per-Local tuple count exceeds u32");
        }
        self.in_run = false;
    }

    /// Materialize one build-side row (post filter/project, post
    /// `eval_build_hash` — null-keyed rows were skipped upstream, C
    /// parity). Payload = the minimal tuple bytes, copied whole; the
    /// storage is self-contained global-heap (survives helper teardown
    /// for rescan reuse, §8).
    pub fn push(&mut self, hashvalue: u32, payload: &[u8]) -> Result<(), BudgetExceeded> {
        assert!(self.in_run, "push outside a run");
        let payload_words = payload.len().div_ceil(8);
        let need = HDR_WORDS + payload_words;

        if self.chunks.last().map_or(true, |c| c.remaining() < need) {
            let mut cap = self
                .chunks
                .last()
                .map_or(CHUNK_MIN_WORDS, |c| (c.words.len() * 2).min(CHUNK_MAX_WORDS));
            cap = cap.max(need);
            assert!(
                self.chunks.len() < MAX_CHUNKS_PER_LOCAL,
                "join build Local chunk index space exhausted"
            );
            let chunk = Chunk::new(cap);
            // Envelope accounting: chunk capacity + the ref word per
            // tuple (flat 8B/tuple charged below).
            if !self.budget.try_charge(chunk.capacity_bytes()) {
                return Err(BudgetExceeded);
            }
            self.chunks.push(chunk);
        }
        if !self.budget.try_charge(8) {
            return Err(BudgetExceeded);
        }

        let chunk_idx = self.chunks.len() - 1;
        let chunk = &mut self.chunks[chunk_idx];
        let off = chunk.used;
        // SAFETY: single-threaded owner writing fresh, in-bounds words.
        unsafe {
            *chunk.word_mut(off) = 0; // next: end of chain
            *chunk.word_mut(off + 1) = ((payload.len() as u64) << 32) | hashvalue as u64;
            *chunk.word_mut(off + 2) = 0; // match flag
            if payload_words > 0 {
                // Last word is pre-zeroed (fresh chunk words start 0 and
                // are never reused), so a partial tail is zero-padded.
                std::ptr::copy_nonoverlapping(
                    payload.as_ptr(),
                    chunk.word_mut(off + HDR_WORDS) as *mut u8,
                    payload.len(),
                );
            }
        }
        chunk.used = off + need;

        let r = pack_ref(self.ordinal, chunk_idx, off);
        self.part_refs[partition_of(hashvalue)].push(r);
        self.tuples += 1;
        Ok(())
    }

    pub fn tuples(&self) -> u64 {
        self.tuples
    }
}

// ---------------------------------------------------------------------------
// SealedBuild (§4): SEAL output → partition-parallel combine.
// ---------------------------------------------------------------------------

pub struct SealedBuild {
    locals: Vec<JoinBuildLocal>,
    /// ordinal → dense index into `locals` (u16::MAX = absent).
    by_ordinal: Box<[u16]>,
    /// All runs across all Locals, ascending by range_start — THE
    /// deterministic combine order.
    run_order: Vec<(u64, u32, u32)>, // (range_start, local, run)
    buckets: Box<[AtomicU64]>,
    log2_nbuckets: u32,
    total_tuples: u64,
}

impl SealedBuild {
    /// SEAL (accept finalize, single-threaded): size the table from the
    /// TRUE tuple count (no parity constraint on nbuckets — §4), charge
    /// the bucket array to the envelope, order the runs.
    pub fn seal(locals: Vec<JoinBuildLocal>, budget: &JoinBudget) -> Result<SealedBuild, BudgetExceeded> {
        let mut by_ordinal = vec![u16::MAX; 256].into_boxed_slice();
        let mut total = 0u64;
        let mut run_order = Vec::new();
        for (li, l) in locals.iter().enumerate() {
            assert!(!l.in_run, "sealed a Local with an open run");
            assert!(
                by_ordinal[l.ordinal as usize] == u16::MAX,
                "duplicate Local ordinal {}",
                l.ordinal
            );
            by_ordinal[l.ordinal as usize] = li as u16;
            total += l.tuples;
            for (ri, run) in l.runs.iter().enumerate() {
                run_order.push((run.range_start, li as u32, ri as u32));
            }
        }
        // Non-empty runs have disjoint ranges ⇒ distinct starts; empty
        // runs may collide on start and contribute nothing — stable sort
        // keeps the outcome well-defined either way.
        run_order.sort_by_key(|&(start, _, _)| start);

        let nbuckets = total
            .next_power_of_two()
            .clamp(MIN_NBUCKETS, MAX_NBUCKETS);
        if !budget.try_charge(nbuckets as usize * 8) {
            return Err(BudgetExceeded);
        }
        let buckets: Vec<AtomicU64> = (0..nbuckets).map(|_| AtomicU64::new(0)).collect();
        Ok(SealedBuild {
            locals,
            by_ordinal,
            run_order,
            buckets: buckets.into_boxed_slice(),
            log2_nbuckets: nbuckets.trailing_zeros(),
            total_tuples: total,
        })
    }

    pub fn partitions(&self) -> u64 {
        PARTITIONS as u64
    }

    pub fn total_tuples(&self) -> u64 {
        self.total_tuples
    }

    #[inline]
    fn chunk(&self, r: u64) -> (&Chunk, usize) {
        let (ord, ci, off) = unpack_ref(r);
        let li = self.by_ordinal[ord];
        debug_assert!(li != u16::MAX, "ref to unknown Local ordinal");
        (&self.locals[li as usize].chunks[ci], off)
    }

    /// Build partition `part`'s bucket range: walk runs in ascending
    /// range order, within a run in materialization order, head-insert.
    /// EXACTLY-ONCE per partition, single writer for the partition's
    /// buckets and its tuples' `next` words (the ParallelSink combine
    /// contract) — hence plain relaxed stores.
    pub fn combine_partition(&self, part: u64) {
        let part = part as usize;
        assert!(part < PARTITIONS);
        for &(_, li, ri) in &self.run_order {
            let l = &self.locals[li as usize];
            let refs = &l.part_refs[part];
            let ri = ri as usize;
            let start = if ri == 0 { 0 } else { l.runs[ri - 1].ends[part] as usize };
            let end = l.runs[ri].ends[part] as usize;
            for &r in &refs[start..end] {
                let (chunk, off) = self.chunk(r);
                let hashvalue = chunk.read(off + 1) as u32;
                debug_assert_eq!(partition_of(hashvalue), part);
                let b = bucket_of(hashvalue, self.log2_nbuckets);
                let old = self.buckets[b].load(Ordering::Relaxed);
                // next := old head's packed ref+1 (0 when empty).
                chunk.atomic(off).store(old >> 16, Ordering::Relaxed);
                self.buckets[b].store(
                    ((r + 1) << 16) | ((old & 0xFFFF) | tag_bit(hashvalue)),
                    Ordering::Relaxed,
                );
            }
        }
    }

    /// Publish (§4 finalize): freeze. O(1) — storage moves, run/ref
    /// bookkeeping drops.
    pub fn finish(self) -> FrozenJoinTable {
        let mut chunk_lists: Vec<Box<[Chunk]>> = Vec::with_capacity(self.locals.len());
        for l in self.locals {
            chunk_lists.push(l.chunks.into_boxed_slice());
        }
        FrozenJoinTable {
            buckets: self.buckets,
            chunk_lists,
            by_ordinal: self.by_ordinal,
            log2_nbuckets: self.log2_nbuckets,
            total_tuples: self.total_tuples,
        }
    }
}

// ---------------------------------------------------------------------------
// FrozenJoinTable (§4/§5): the probe/fill face.
// ---------------------------------------------------------------------------

pub struct FrozenJoinTable {
    buckets: Box<[AtomicU64]>,
    chunk_lists: Vec<Box<[Chunk]>>,
    by_ordinal: Box<[u16]>,
    log2_nbuckets: u32,
    total_tuples: u64,
}

impl FrozenJoinTable {
    pub fn nbuckets(&self) -> usize {
        self.buckets.len()
    }

    pub fn total_tuples(&self) -> u64 {
        self.total_tuples
    }

    #[inline]
    fn chunk(&self, r: u64) -> (&Chunk, usize) {
        let (ord, ci, off) = unpack_ref(r);
        (&self.chunk_lists[self.by_ordinal[ord] as usize][ci], off)
    }

    /// The probe entry: the hash's bucket chain, tag-prefiltered (a tag
    /// miss returns an empty iterator after ONE bucket-word read).
    /// Yields every chain tuple in serial-identical order; the caller
    /// filters by hashvalue + quals (C's probe discipline).
    pub fn chain(&self, hashvalue: u32) -> ChainIter<'_> {
        let word = self.buckets[bucket_of(hashvalue, self.log2_nbuckets)].load(Ordering::Relaxed);
        let head = if word & tag_bit(hashvalue) != 0 { word >> 16 } else { 0 };
        ChainIter { table: self, next_packed: head }
    }

    /// Unfiltered bucket walk (fill phase + tests).
    pub fn bucket_chain(&self, bucket: usize) -> ChainIter<'_> {
        ChainIter {
            table: self,
            next_packed: self.buckets[bucket].load(Ordering::Relaxed) >> 16,
        }
    }

    /// Partition `part`'s exclusive bucket range (§4 layout).
    pub fn partition_buckets(&self, part: u64) -> Range<usize> {
        let per = self.buckets.len() / PARTITIONS;
        let p = part as usize;
        p * per..(p + 1) * per
    }

    /// The right-fill walk (§5): never-matched tuples of one partition,
    /// bucket order then chain order — `scan_hash_table_for_unmatched`'s
    /// shape over the frozen layout. Run after the probe set's
    /// completion barrier.
    pub fn unmatched_in_partition(&self, part: u64) -> impl Iterator<Item = TupleRef<'_>> {
        self.partition_buckets(part)
            .flat_map(move |b| self.bucket_chain(b))
            .filter(|t| !t.matched())
    }
}

/// A borrowed view of one build tuple.
#[derive(Clone, Copy)]
pub struct TupleRef<'t> {
    table: &'t FrozenJoinTable,
    r: u64,
}

impl<'t> TupleRef<'t> {
    #[inline]
    pub fn hashvalue(&self) -> u32 {
        let (chunk, off) = self.table.chunk(self.r);
        chunk.read(off + 1) as u32
    }

    #[inline]
    pub fn payload(&self) -> &'t [u8] {
        let (chunk, off) = self.table.chunk(self.r);
        let len = (chunk.read(off + 1) >> 32) as usize;
        // SAFETY: payload words are frozen (never written post-seal);
        // the chunk (and thus the bytes) lives as long as the table.
        unsafe {
            std::slice::from_raw_parts(chunk.word_mut(off + HDR_WORDS) as *const u8, len)
        }
    }

    /// Right-fill match flag: idempotent monotonic set (racy-OK — the C
    /// PHJ discipline; visibility to the fill phase is the probe task
    /// set's completion barrier).
    #[inline]
    pub fn set_matched(&self) {
        let (chunk, off) = self.table.chunk(self.r);
        chunk.atomic(off + 2).store(1, Ordering::Relaxed);
    }

    /// RIGHT_SEMI's emit-once discipline: true ⇔ this call won the flag.
    #[inline]
    pub fn test_and_set_matched(&self) -> bool {
        let (chunk, off) = self.table.chunk(self.r);
        chunk.atomic(off + 2).swap(1, Ordering::Relaxed) == 0
    }

    #[inline]
    pub fn matched(&self) -> bool {
        let (chunk, off) = self.table.chunk(self.r);
        chunk.atomic(off + 2).load(Ordering::Relaxed) != 0
    }
}

pub struct ChainIter<'t> {
    table: &'t FrozenJoinTable,
    next_packed: u64, // ref+1; 0 = end
}

impl<'t> Iterator for ChainIter<'t> {
    type Item = TupleRef<'t>;

    fn next(&mut self) -> Option<TupleRef<'t>> {
        if self.next_packed == 0 {
            return None;
        }
        let r = self.next_packed - 1;
        let (chunk, off) = self.table.chunk(r);
        self.next_packed = chunk.atomic(off).load(Ordering::Relaxed);
        Some(TupleRef { table: self.table, r })
    }
}

// ---------------------------------------------------------------------------
// Tests — the inc-1 gate. The determinism property tests are the
// coordinator-ratified conditions (a)/(b)/(c).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic splitmix64-derived row stream: granule g yields
    /// `rows_per_granule` rows of (hashvalue, payload). Duplicate-heavy:
    /// hash keys are drawn from a small space so chains carry many
    /// equal-hash tuples.
    fn mix(mut x: u64) -> u64 {
        x = x.wrapping_add(0x9e3779b97f4a7c15);
        x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
        x ^ (x >> 31)
    }

    #[derive(Clone)]
    struct Dataset {
        granules: u64,
        rows_per_granule: u64,
        key_space: u64, // small ⇒ duplicate-heavy
        seed: u64,
        force_partition: Option<u8>, // all-one-partition degenerate
    }

    impl Dataset {
        fn rows_of(&self, g: u64) -> Vec<(u32, Vec<u8>)> {
            (0..self.rows_per_granule)
                .map(|i| {
                    let id = g * self.rows_per_granule + i;
                    let key = mix(self.seed ^ id) % self.key_space;
                    let mut h = mix(key.wrapping_mul(0x517c_c1b7_2722_0a95)) as u32;
                    if let Some(p) = self.force_partition {
                        h = (h & 0x00FF_FFFF) | ((p as u32) << 24);
                    }
                    // Payload: the global row id + variable tail (odd
                    // lengths exercise word padding).
                    let mut payload = id.to_le_bytes().to_vec();
                    payload.extend(std::iter::repeat(0xA5u8).take((id % 13) as usize));
                    (h, payload)
                })
                .collect()
        }

        fn all_rows(&self) -> Vec<(u32, Vec<u8>)> {
            (0..self.granules).flat_map(|g| self.rows_of(g)).collect()
        }
    }

    /// The serial oracle: insert all rows in global scan order at chain
    /// head, same bucket function. `chains[b]` = head-first sequence.
    fn reference_chains(rows: &[(u32, Vec<u8>)], log2_nbuckets: u32) -> Vec<Vec<(u32, Vec<u8>)>> {
        let mut chains: Vec<Vec<(u32, Vec<u8>)>> = vec![Vec::new(); 1 << log2_nbuckets];
        for (h, p) in rows {
            chains[bucket_of(*h, log2_nbuckets)].insert(0, (*h, p.clone()));
        }
        chains
    }

    /// A claim schedule: ordered per-local lists of granule ranges.
    /// Ranges are disjoint and cover 0..granules; per-local claim order
    /// is arbitrary (the scheme must not depend on it).
    type Schedule = Vec<Vec<Range<u64>>>;

    fn build_from_schedule(
        ds: &Dataset,
        schedule: &Schedule,
        budget: &Arc<JoinBudget>,
    ) -> Result<SealedBuild, BudgetExceeded> {
        let mut locals = Vec::new();
        for (w, claims) in schedule.iter().enumerate() {
            let mut l = JoinBuildLocal::new(w, Arc::clone(budget));
            for range in claims {
                l.begin_run(range.start);
                for g in range.clone() {
                    for (h, p) in ds.rows_of(g) {
                        l.push(h, &p)?;
                    }
                }
                l.end_run();
            }
            locals.push(l);
        }
        SealedBuild::seal(locals, budget)
    }

    fn frozen_chains(t: &FrozenJoinTable) -> Vec<Vec<(u32, Vec<u8>)>> {
        (0..t.nbuckets())
            .map(|b| {
                t.bucket_chain(b)
                    .map(|tr| (tr.hashvalue(), tr.payload().to_vec()))
                    .collect()
            })
            .collect()
    }

    fn combine_all_serial(s: &SealedBuild) {
        for p in 0..PARTITIONS as u64 {
            s.combine_partition(p);
        }
    }

    fn combine_all_parallel(s: &SealedBuild, threads: usize) {
        let next = AtomicU64::new(0);
        std::thread::scope(|scope| {
            for _ in 0..threads {
                scope.spawn(|| loop {
                    let p = next.fetch_add(1, Ordering::Relaxed);
                    if p >= PARTITIONS as u64 {
                        break;
                    }
                    s.combine_partition(p);
                });
            }
        });
    }

    fn assert_serial_identical(ds: &Dataset, schedule: &Schedule, parallel_combine: bool) {
        let budget = JoinBudget::unlimited();
        let sealed = build_from_schedule(ds, schedule, &budget).expect("unlimited budget");
        if parallel_combine {
            combine_all_parallel(&sealed, 8);
        } else {
            combine_all_serial(&sealed);
        }
        let l2 = sealed.log2_nbuckets;
        let t = sealed.finish();
        let expect = reference_chains(&ds.all_rows(), l2);
        let got = frozen_chains(&t);
        assert_eq!(t.total_tuples(), ds.granules * ds.rows_per_granule);
        assert_eq!(got, expect, "chains diverge from the serial oracle");
    }

    fn ds_default() -> Dataset {
        Dataset {
            granules: 64,
            rows_per_granule: 37,
            key_space: 97, // duplicate-heavy
            seed: 0xD1CE,
            force_partition: None,
        }
    }

    /// Split 0..granules into consecutive ranges with the given sizes.
    fn ranges_of_sizes(granules: u64, sizes: impl IntoIterator<Item = u64>) -> Vec<Range<u64>> {
        let mut out = Vec::new();
        let mut at = 0;
        for s in sizes {
            if at >= granules {
                break;
            }
            let end = (at + s).min(granules);
            out.push(at..end);
            at = end;
        }
        if at < granules {
            out.push(at..granules);
        }
        out
    }

    /// Deal `ranges` to `workers` locals; `order_seed` shuffles the
    /// per-local claim order (workers need not claim ascending).
    fn deal(ranges: Vec<Range<u64>>, workers: usize, order_seed: u64) -> Schedule {
        let mut sched: Schedule = vec![Vec::new(); workers];
        for (i, r) in ranges.into_iter().enumerate() {
            sched[(mix(order_seed ^ i as u64) as usize) % workers].push(r);
        }
        for (w, claims) in sched.iter_mut().enumerate() {
            // Pseudo-random per-local order.
            let n = claims.len();
            for i in (1..n).rev() {
                claims.swap(i, (mix(order_seed ^ (w as u64) << 32 ^ i as u64) as usize) % (i + 1));
            }
        }
        sched
    }

    // ---- condition (a): adversarial claim orders ----

    #[test]
    fn single_worker_takes_all_one_run() {
        let ds = ds_default();
        assert_serial_identical(&ds, &vec![vec![0..ds.granules]], false);
    }

    #[test]
    fn single_worker_takes_all_many_runs_out_of_order() {
        let ds = ds_default();
        // One worker, granule-sized runs claimed in reversed order.
        let claims: Vec<Range<u64>> = (0..ds.granules).rev().map(|g| g..g + 1).collect();
        assert_serial_identical(&ds, &vec![claims], false);
    }

    #[test]
    fn maximal_interleave_round_robin() {
        let ds = ds_default();
        let workers = 7;
        let mut sched: Schedule = vec![Vec::new(); workers];
        for g in 0..ds.granules {
            sched[g as usize % workers].push(g..g + 1);
        }
        assert_serial_identical(&ds, &sched, true);
    }

    #[test]
    fn randomized_schedules_match_oracle() {
        let ds = ds_default();
        for seed in 0..24u64 {
            let sizes = (0..).map(|i| (mix(seed ^ i) % 7) + 1);
            let sched = deal(ranges_of_sizes(ds.granules, sizes.take(64)), 1 + (seed as usize % 9), seed);
            assert_serial_identical(&ds, &sched, seed % 2 == 0);
        }
    }

    // ---- condition (b): morsel resize boundaries (ramp/photo-finish) ----

    #[test]
    fn ramp_and_photo_finish_sizing_compose_identically() {
        let ds = ds_default();
        // Exponential startup ramp (1,2,4,8,...) then photo-finish
        // size-1 tails — the adaptive sizing shape.
        let ramp = ranges_of_sizes(
            ds.granules,
            (0..5).map(|i| 1u64 << i).chain(std::iter::repeat(1)),
        );
        // The same space under flat mid-size runs.
        let flat = ranges_of_sizes(ds.granules, std::iter::repeat(5));
        // And one whole-space run.
        let whole = vec![0..ds.granules];
        for (i, ranges) in [ramp, flat, whole].into_iter().enumerate() {
            let sched = deal(ranges, 4, 0xBEEF ^ i as u64);
            assert_serial_identical(&ds, &sched, true);
        }
    }

    // ---- condition (c): degenerates ----

    #[test]
    fn empty_build() {
        let budget = JoinBudget::unlimited();
        // No locals at all.
        let sealed = SealedBuild::seal(Vec::new(), &budget).unwrap();
        combine_all_serial(&sealed);
        let t = sealed.finish();
        assert_eq!(t.total_tuples(), 0);
        assert!(frozen_chains(&t).iter().all(|c| c.is_empty()));
        assert_eq!(t.chain(0xDEAD_BEEF).count(), 0);

        // Locals that forked but saw only empty runs.
        let mut l = JoinBuildLocal::new(3, Arc::clone(&budget));
        l.begin_run(10);
        l.end_run();
        let sealed = SealedBuild::seal(vec![l], &budget).unwrap();
        combine_all_serial(&sealed);
        assert_eq!(sealed_total(&sealed), 0);
    }

    fn sealed_total(s: &SealedBuild) -> u64 {
        s.total_tuples()
    }

    #[test]
    fn empty_runs_interleaved_are_inert() {
        let ds = ds_default();
        let budget = JoinBudget::unlimited();
        let mut with_empties = JoinBuildLocal::new(0, Arc::clone(&budget));
        for g in 0..ds.granules {
            // Every other claim yields no granules (range start recorded,
            // nothing pushed) — e.g. a fully filtered morsel.
            with_empties.begin_run(g);
            if g % 2 == 0 {
                for (h, p) in ds.rows_of(g) {
                    with_empties.push(h, &p).unwrap();
                }
            }
            with_empties.end_run();
        }
        let sealed = SealedBuild::seal(vec![with_empties], &budget).unwrap();
        combine_all_serial(&sealed);
        let l2 = sealed.log2_nbuckets;
        let t = sealed.finish();
        let rows: Vec<_> = (0..ds.granules)
            .filter(|g| g % 2 == 0)
            .flat_map(|g| ds.rows_of(g))
            .collect();
        assert_eq!(frozen_chains(&t), reference_chains(&rows, l2));
    }

    #[test]
    fn all_one_partition() {
        let mut ds = ds_default();
        ds.force_partition = Some(0xAB);
        let sched = deal(ranges_of_sizes(ds.granules, std::iter::repeat(3)), 5, 42);
        assert_serial_identical(&ds, &sched, true);
    }

    #[test]
    fn identical_full_hash_duplicates_keep_scan_order() {
        // Every row hashes identically: one bucket, one chain, order =
        // exactly reversed global scan order.
        let budget = JoinBudget::unlimited();
        let mut a = JoinBuildLocal::new(0, Arc::clone(&budget));
        let mut b = JoinBuildLocal::new(1, Arc::clone(&budget));
        let h = 0x1234_5678u32;
        // Worker b claims the SECOND range first (arrival order must not
        // matter).
        b.begin_run(4);
        for id in 4u64..8 {
            b.push(h, &id.to_le_bytes()).unwrap();
        }
        b.end_run();
        a.begin_run(0);
        for id in 0u64..4 {
            a.push(h, &id.to_le_bytes()).unwrap();
        }
        a.end_run();
        let sealed = SealedBuild::seal(vec![a, b], &budget).unwrap();
        combine_all_serial(&sealed);
        let t = sealed.finish();
        let got: Vec<u64> = t
            .chain(h)
            .map(|tr| u64::from_le_bytes(tr.payload().try_into().unwrap()))
            .collect();
        assert_eq!(got, vec![7, 6, 5, 4, 3, 2, 1, 0]);
    }

    // ---- storage/probe mechanics ----

    #[test]
    fn payload_roundtrip_and_tag_no_false_negatives() {
        let ds = Dataset { granules: 16, rows_per_granule: 11, key_space: 1 << 30, seed: 7, force_partition: None };
        let budget = JoinBudget::unlimited();
        let sealed = build_from_schedule(&ds, &vec![vec![0..16]], &budget).unwrap();
        combine_all_serial(&sealed);
        let t = sealed.finish();
        for (h, p) in ds.all_rows() {
            // Tag filter must never hide a present hash.
            let found = t
                .chain(h)
                .any(|tr| tr.hashvalue() == h && tr.payload() == &p[..]);
            assert!(found, "tuple lost (hash {h:#x})");
        }
    }

    #[test]
    fn chunk_growth_across_many_chunks() {
        // Payloads big enough to force several chunk allocations.
        let budget = JoinBudget::unlimited();
        let mut l = JoinBuildLocal::new(0, Arc::clone(&budget));
        let payload = vec![0x5Au8; 40_000];
        l.begin_run(0);
        for i in 0..64u32 {
            let mut p = payload.clone();
            p[0] = i as u8;
            l.push(mix(i as u64) as u32, &p).unwrap();
        }
        l.end_run();
        assert!(l.chunks.len() > 1, "expected chunk growth");
        let sealed = SealedBuild::seal(vec![l], &budget).unwrap();
        combine_all_serial(&sealed);
        let t = sealed.finish();
        let mut seen = 0;
        for b in 0..t.nbuckets() {
            for tr in t.bucket_chain(b) {
                assert_eq!(tr.payload().len(), 40_000);
                seen += 1;
            }
        }
        assert_eq!(seen, 64);
    }

    #[test]
    fn zero_length_payload() {
        let budget = JoinBudget::unlimited();
        let mut l = JoinBuildLocal::new(0, Arc::clone(&budget));
        l.begin_run(0);
        l.push(0xFEED_F00D, &[]).unwrap();
        l.end_run();
        let sealed = SealedBuild::seal(vec![l], &budget).unwrap();
        combine_all_serial(&sealed);
        let t = sealed.finish();
        let tr = t.chain(0xFEED_F00D).next().expect("present");
        assert_eq!(tr.payload(), &[] as &[u8]);
    }

    // ---- budget (§6 enforcement half) ----

    #[test]
    fn budget_crossing_refuses_on_push() {
        let budget = JoinBudget::new(CHUNK_MIN_WORDS * 8 + 64);
        let mut l = JoinBuildLocal::new(0, Arc::clone(&budget));
        l.begin_run(0);
        let mut crossed = false;
        for i in 0..10_000u64 {
            if l.push(mix(i) as u32, &i.to_le_bytes()).is_err() {
                crossed = true;
                break;
            }
        }
        assert!(crossed, "envelope crossing must surface as BudgetExceeded");
    }

    #[test]
    fn budget_crossing_refuses_at_seal() {
        // Fits during accept, crossed by the bucket array at SEAL.
        let budget = JoinBudget::new(CHUNK_MIN_WORDS * 8 + 16 * 1024);
        let mut l = JoinBuildLocal::new(0, Arc::clone(&budget));
        l.begin_run(0);
        for i in 0..1000u64 {
            l.push(mix(i) as u32, &i.to_le_bytes()).unwrap();
        }
        l.end_run();
        assert_eq!(SealedBuild::seal(vec![l], &budget).err(), Some(BudgetExceeded));
    }

    // ---- match flags (§5; loom-adjacent stress — the real barrier is
    // the runtime's Loom-verified task-set completion) ----

    #[test]
    fn match_flags_concurrent_probe_then_fill_exact_set() {
        let ds = Dataset { granules: 32, rows_per_granule: 16, key_space: 64, seed: 99, force_partition: None };
        let budget = JoinBudget::unlimited();
        let sealed = build_from_schedule(&ds, &deal(ranges_of_sizes(32, std::iter::repeat(3)), 4, 5), &budget).unwrap();
        combine_all_parallel(&sealed, 4);
        let t = sealed.finish();

        // "Probe": 8 threads racily mark every tuple whose payload row
        // id is even (many threads hit the same tuples — idempotent).
        std::thread::scope(|scope| {
            for w in 0..8 {
                let t = &t;
                scope.spawn(move || {
                    for b in 0..t.nbuckets() {
                        if b % 2 != w % 2 {
                            continue; // overlapping-but-different coverage
                        }
                        for tr in t.bucket_chain(b) {
                            let id = u64::from_le_bytes(tr.payload()[..8].try_into().unwrap());
                            if id % 2 == 0 {
                                tr.set_matched();
                            }
                        }
                    }
                });
            }
        });
        // "Fill" after the join (the barrier): exactly the odd rows
        // remain, in bucket-then-chain order per partition.
        let mut unmatched = 0u64;
        for p in 0..PARTITIONS as u64 {
            for tr in t.unmatched_in_partition(p) {
                let id = u64::from_le_bytes(tr.payload()[..8].try_into().unwrap());
                assert_eq!(id % 2, 1);
                unmatched += 1;
            }
        }
        assert_eq!(unmatched, 32 * 16 / 2);
    }

    #[test]
    fn test_and_set_matched_emits_once() {
        let budget = JoinBudget::unlimited();
        let mut l = JoinBuildLocal::new(0, Arc::clone(&budget));
        l.begin_run(0);
        l.push(0xC0FF_EE00, b"once").unwrap();
        l.end_run();
        let sealed = SealedBuild::seal(vec![l], &budget).unwrap();
        combine_all_serial(&sealed);
        let t = sealed.finish();
        let wins = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let (t, wins) = (&t, &wins);
                scope.spawn(move || {
                    let tr = t.chain(0xC0FF_EE00).next().unwrap();
                    if tr.test_and_set_matched() {
                        wins.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
        });
        assert_eq!(wins.load(Ordering::Relaxed), 1, "RIGHT_SEMI emit-once violated");
    }

    // ---- soak: larger randomized run vs the oracle ----

    #[test]
    fn soak_100k_random_schedule() {
        let ds = Dataset {
            granules: 256,
            rows_per_granule: 400, // 102,400 tuples
            key_space: 5000,
            seed: 0x50CA,
            force_partition: None,
        };
        let sizes = (0..).map(|i| (mix(0xFACE ^ i) % 9) + 1);
        let sched = deal(ranges_of_sizes(ds.granules, sizes.take(200)), 16, 0xFACE);
        assert_serial_identical(&ds, &sched, true);
    }
}
