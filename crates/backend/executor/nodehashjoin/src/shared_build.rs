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
//! # Chain order: reproducible, NOT a correctness contract
//!
//! AMENDED per Michael's 2026-07-13 directive ("we shouldn't have to
//! guarantee the same order", m3-joins.md §4): order-insensitive
//! emission is the baseline join semantics and the gate oracle is
//! tie-normalized comparison. The run-ordered walk below is retained
//! only because it is free and makes failures reproducible; any future
//! increment may replace it with naive per-Local insertion order, and
//! no design (spill, skew, right-fill, probe) may depend on chain
//! order. Mechanically:
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
//! - `CombinePlan::combine_partition(part, locals)` is called EXACTLY ONCE per
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

/// Packed tuple reference: ordinal(15) | chunk(8) | word-offset(25) = 48
/// bits. Stored shifted/+1 so 0 can mean "empty"/"end of chain".
///
/// The 48-bit total is LOAD-BEARING: a bucket word is `(ref+1) << 16 |
/// tag(16)` (§4), so the ref may never exceed 48 bits. The DOP-192
/// readiness repack moved headroom from the offset (32 bits addressed
/// 32GB chunks; `CHUNK_MAX_WORDS` is 2^21 words, and an over-`need`
/// chunk holds exactly one tuple at offset 0, so 25 bits ≫ suffice)
/// into the ordinal: 8 bits capped the sink worker index at 256, which
/// an absolute runtime worker index crosses at nthreads ≥ 193 (index
/// 192 + the 64-lane pin board). 15 bits ⇒ 32768 ordinals.
const REF_OFFSET_BITS: u32 = 25;
const REF_CHUNK_BITS: u32 = 8;
const REF_ORDINAL_BITS: u32 = 48 - REF_CHUNK_BITS - REF_OFFSET_BITS; // 15

/// Ordinal (sink worker index) space of the packed ref: 32768.
pub const MAX_ORDINALS: usize = 1 << REF_ORDINAL_BITS;

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
fn pack_ref(ordinal: u16, chunk: usize, off_words: usize) -> u64 {
    debug_assert!((ordinal as usize) < MAX_ORDINALS);
    debug_assert!(chunk < MAX_CHUNKS_PER_LOCAL);
    debug_assert!(off_words < (1 << REF_OFFSET_BITS));
    ((ordinal as u64) << (REF_CHUNK_BITS + REF_OFFSET_BITS))
        | ((chunk as u64) << REF_OFFSET_BITS)
        | off_words as u64
}

#[inline]
fn unpack_ref(r: u64) -> (usize, usize, usize) {
    (
        (r >> (REF_CHUNK_BITS + REF_OFFSET_BITS)) as usize,
        ((r >> REF_OFFSET_BITS) & ((1 << REF_CHUNK_BITS) - 1)) as usize,
        (r & ((1 << REF_OFFSET_BITS) - 1)) as usize,
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

    /// Optional charge (HJPROBE-V2 dense seat; the single-pass directory):
    /// on a crossing the charge is BACKED OUT and the caller simply forgoes
    /// the optional structure — never a refusal (the structure is an
    /// accelerator or has a documented fallback, not a table half).
    fn try_charge_optional(&self, n: usize) -> bool {
        if self.try_charge(n) {
            return true;
        }
        self.release(n);
        false
    }

    /// Return `n` previously charged bytes to the envelope (a backed-out
    /// optional charge, or an array superseded by a grown replacement).
    fn release(&self, n: usize) {
        let prev = self.used.fetch_sub(n, Ordering::Relaxed);
        debug_assert!(prev >= n, "budget release exceeds charges");
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
        Chunk { words: v.into_boxed_slice() }
    }

    #[inline]
    fn capacity_bytes(&self) -> usize {
        self.words.len() * 8
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

/// HJPROBE-V2 dense-seat key sentinel: a NULL build key (never matches any
/// probe — C parity — so it is simply left out of every seat chain).
pub const NULL_KEY: i64 = i64::MIN;

/// Per-Local dense-key tracking (HJPROBE-V2, notes/se-hjprobe-v2.md §4.3
/// increment 1 — the legacy nodehash `KeyTrack` idiom on the lane Local):
/// `part_keys[p]` is in EXACT lockstep with `part_refs[p]`, so the seat
/// build can enumerate (ref, key) pairs in the combine walk's order.
struct DenseKeys {
    part_keys: Vec<Vec<i64>>,
}

pub struct JoinBuildLocal {
    ordinal: u16,
    /// Arc so the frozen table can adopt the storage by reference while the
    /// sink plumbing still owns (and later drops) the Locals themselves —
    /// the ParallelSink contract hands `finalize` only `&[Local]`.
    chunks: Vec<Arc<Chunk>>,
    /// Bump offset into the LAST chunk (chunks before it are full).
    cur_used: usize,
    /// Per-partition tuple refs in materialization (= scan) order.
    part_refs: Vec<Vec<u64>>,
    runs: Vec<RunHeader>,
    in_run: bool,
    tuples: u64,
    budget: Arc<JoinBudget>,
    /// Chunk growth ceiling in words (M3.5 leaf builds cap this so the
    /// PLAN-BATCHES capacity model's last-chunk waste term stays small).
    chunk_cap_words: usize,
    /// HJPROBE-V2: armed dense-key tracking (None = v1, byte-identical).
    dense_keys: Option<DenseKeys>,
    /// SINGLE-PASS (Phase 1a): the shared directory this Local inserts into
    /// directly during accept (None = the two-pass materialize→combine
    /// default). Attached at fork under the PGRUST_RUNTIME_HJ_SINGLEPASS
    /// kill switch; when set, `push` bypasses `part_refs`/`runs` entirely.
    shared_dir: Option<Arc<SharedBuildDir>>,
    /// SE-MBSEAT: single-pass dense-key tracking — flat `(packed_ref, key)`
    /// pairs in materialization order (NO chain-order lockstep: the
    /// single-pass chains are concurrent-nondeterministic already, and the
    /// multibuild consumer is order-insensitive by construction — the
    /// 2026-07-13 order directive's baseline). `Some` only on a Local with
    /// an attached shared directory ([`JoinBuildLocal::arm_singlepass_keys`]);
    /// consumed by [`build_seat_single_pass`] at the freeze barrier.
    sp_keys: Option<Vec<(u64, i64)>>,
}

impl JoinBuildLocal {
    /// `ordinal` = the sink worker index (worker-indexed Local slots,
    /// R3 pinned regime); must be < [`MAX_ORDINALS`] = 32768 (asserted —
    /// the runtime passes its ABSOLUTE worker index, so this must hold
    /// for nthreads + pin-board lanes, not just the tested DOP).
    pub fn new(ordinal: usize, budget: Arc<JoinBudget>) -> JoinBuildLocal {
        JoinBuildLocal::with_chunk_cap(ordinal, budget, CHUNK_MAX_WORDS)
    }

    /// [`JoinBuildLocal::new`] with a chunk-growth ceiling (M3.5 batch
    /// builds; `cap_words` clamps to the default ladder's bounds).
    pub fn with_chunk_cap(
        ordinal: usize,
        budget: Arc<JoinBudget>,
        cap_words: usize,
    ) -> JoinBuildLocal {
        assert!(ordinal < MAX_ORDINALS, "join build Local ordinal {ordinal} out of ref range");
        JoinBuildLocal {
            ordinal: ordinal as u16,
            chunks: Vec::new(),
            cur_used: 0,
            part_refs: (0..PARTITIONS).map(|_| Vec::new()).collect(),
            runs: Vec::new(),
            in_run: false,
            tuples: 0,
            budget,
            chunk_cap_words: cap_words.clamp(CHUNK_MIN_WORDS, CHUNK_MAX_WORDS),
            dense_keys: None,
            shared_dir: None,
            sp_keys: None,
        }
    }

    pub fn ordinal(&self) -> usize {
        self.ordinal as usize
    }

    /// SINGLE-PASS (Phase 1a): attach the shared directory. Must run before
    /// the first push (asserted) and is mutually exclusive with dense-key
    /// arming — the runtime forks a Local into exactly one mode.
    pub fn attach_shared_dir(&mut self, dir: Arc<SharedBuildDir>) {
        assert!(self.tuples == 0, "attach_shared_dir after pushes");
        assert!(self.dense_keys.is_none(), "single-pass is incompatible with the dense seat");
        self.shared_dir = Some(dir);
    }

    /// Whether this Local inserts single-pass (the accept site's dispatch;
    /// also the runtime's gate to keep the dense seat OFF under single-pass).
    #[inline(always)]
    pub fn single_pass(&self) -> bool {
        self.shared_dir.is_some()
    }

    /// SE-MBSEAT: arm single-pass dense-key tracking. Idempotent; must run
    /// before the Local's first push (asserted) — the runtime arms on the
    /// Local's FIRST build morsel of a seat-targeted table, so a Local with
    /// tuples is armed-or-never (the all-or-none law across tuple-bearing
    /// Locals, [`build_seat_single_pass`]'s input contract). Requires the
    /// attached shared directory (single-pass only; the two-pass seat rides
    /// [`JoinBuildLocal::arm_dense_keys`]).
    pub fn arm_singlepass_keys(&mut self) {
        if self.sp_keys.is_some() {
            return;
        }
        assert!(self.tuples == 0, "single-pass key arming after pushes");
        assert!(
            self.shared_dir.is_some(),
            "single-pass key tracking requires an attached shared directory"
        );
        self.sp_keys = Some(Vec::new());
    }

    /// Whether this Local tracks single-pass keys (the accept dispatch).
    #[inline(always)]
    pub fn singlepass_keys_armed(&self) -> bool {
        self.sp_keys.is_some()
    }

    /// HJPROBE-V2: arm dense-key tracking. Idempotent; must run before the
    /// Local's first push (asserted) — the runtime arms on the Local's
    /// FIRST build morsel, so a Local with tuples is armed-or-never.
    pub fn arm_dense_keys(&mut self) {
        if self.dense_keys.is_some() {
            return;
        }
        assert!(self.tuples == 0, "dense-key arming after pushes");
        assert!(self.shared_dir.is_none(), "the dense seat is incompatible with single-pass build");
        self.dense_keys = Some(DenseKeys {
            part_keys: (0..PARTITIONS).map(|_| Vec::new()).collect(),
        });
    }

    /// Whether this Local tracks dense keys (the accept site's dispatch).
    #[inline(always)]
    pub fn dense_armed(&self) -> bool {
        self.dense_keys.is_some()
    }

    /// [`JoinBuildLocal::push`] + the dense-key record. Two-pass: keys in
    /// EXACT lockstep with `part_refs` (the chain-order seat's contract).
    /// Single-pass (SE-MBSEAT): flat `(packed_ref, key)` pairs, order-free.
    /// `NULL_KEY` = SQL NULL, kept out of seat chains either way.
    pub fn push_keyed(
        &mut self,
        hashvalue: u32,
        payload: &[u8],
        key: i64,
    ) -> Result<(), BudgetExceeded> {
        if self.shared_dir.is_some() {
            let r = self.push_single_pass(hashvalue, payload)?;
            self.sp_keys
                .as_mut()
                .expect("push_keyed on an unarmed single-pass Local")
                .push((r, key));
            return Ok(());
        }
        self.push(hashvalue, payload)?;
        self.dense_keys
            .as_mut()
            .expect("push_keyed on an unarmed Local")
            .part_keys[partition_of(hashvalue)]
            .push(key);
        Ok(())
    }

    /// Visit every materialized tuple as `(hashvalue, payload)` — the M3.5
    /// batch-0 demote dump (§5.2: a dumped batch 0 becomes an ordinary file
    /// batch). Partition order, materialization order within.
    pub fn drain_records<E>(
        &self,
        mut f: impl FnMut(u32, &[u8]) -> Result<(), E>,
    ) -> Result<(), E> {
        for refs in &self.part_refs {
            for &r in refs {
                let (ord, ci, off) = unpack_ref(r);
                debug_assert_eq!(ord, self.ordinal as usize);
                let chunk = &self.chunks[ci];
                let w1 = chunk.read(off + 1);
                let (len, hashvalue) = ((w1 >> 32) as usize, w1 as u32);
                // SAFETY: frozen payload words of this Local's own chunk
                // (single-threaded caller; accept has ended).
                let payload = unsafe {
                    std::slice::from_raw_parts(chunk.word_mut(off + HDR_WORDS) as *const u8, len)
                };
                f(hashvalue, payload)?;
            }
        }
        Ok(())
    }

    /// Drop all materialized storage (post-dump). Runs are cleared too; the
    /// Local behaves as freshly forked with no open run.
    pub fn reset(&mut self) {
        assert!(!self.in_run, "reset inside an open run");
        self.chunks.clear();
        self.cur_used = 0;
        for refs in &mut self.part_refs {
            refs.clear();
        }
        if let Some(dk) = &mut self.dense_keys {
            for keys in &mut dk.part_keys {
                keys.clear();
            }
        }
        if let Some(sp) = &mut self.sp_keys {
            sp.clear();
        }
        self.runs.clear();
        self.tuples = 0;
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
        // SINGLE-PASS (Phase 1a): a Local with an attached shared directory
        // inserts EACH tuple directly into the shared table via atomic CAS
        // during the build scan (Umbra/PG Parallel Hash lineage) — no
        // per-partition ref list, no second COMBINE bandwidth pass. The
        // attach is a fork-time decision (JoinBuildSink under the
        // PGRUST_RUNTIME_HJ_SINGLEPASS kill switch), so every accept call
        // site is unchanged.
        if self.shared_dir.is_some() {
            return self.push_single_pass(hashvalue, payload).map(|_| ());
        }
        let (chunk_idx, off) = self.materialize(hashvalue, payload)?;
        let r = pack_ref(self.ordinal, chunk_idx, off);
        self.part_refs[partition_of(hashvalue)].push(r);
        self.tuples += 1;
        Ok(())
    }

    /// Materialize one row into this Local's chunk arena and return its
    /// `(chunk_idx, word_offset)`. Charges the envelope (chunk capacity + 8B
    /// per tuple). Header words are written (next=0, len|hash, match=0); the
    /// caller decides how the tuple is INDEXED (two-pass: recorded in
    /// `part_refs` for a later COMBINE; single-pass: CAS-linked immediately).
    #[inline]
    fn materialize(&mut self, hashvalue: u32, payload: &[u8]) -> Result<(usize, usize), BudgetExceeded> {
        assert!(self.in_run, "push outside a run");
        let payload_words = payload.len().div_ceil(8);
        let need = HDR_WORDS + payload_words;

        if self.chunks.last().map_or(true, |c| c.words.len() - self.cur_used < need) {
            let mut cap = self
                .chunks
                .last()
                .map_or(CHUNK_MIN_WORDS, |c| (c.words.len() * 2).min(self.chunk_cap_words));
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
            self.chunks.push(Arc::new(chunk));
            self.cur_used = 0;
        }
        if !self.budget.try_charge(8) {
            return Err(BudgetExceeded);
        }

        let chunk_idx = self.chunks.len() - 1;
        let chunk = &self.chunks[chunk_idx];
        let off = self.cur_used;
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
        self.cur_used = off + need;
        Ok((chunk_idx, off))
    }

    /// Single-pass insert (Phase 1a): materialize, then CAS-link the tuple at
    /// its bucket's chain head in the attached shared directory. The tuple's
    /// `next` word (W0, in this Local's own chunk) is the CAS's per-tuple
    /// scratch — only this worker writes it, so no writer touches another
    /// chain word during build; cross-worker visibility of the finished
    /// chains is the runtime task-set completion barrier (as in the two-pass
    /// combine). TWO-PASS dense-key tracking (`dense_keys`) is NOT supported
    /// here — the single-join HJPROBE-V2 seat's byte-parity proof needs the
    /// reproducible chain order the concurrent insert cannot give. The
    /// SE-MBSEAT order-free key tracking (`sp_keys`) IS supported: it rides
    /// [`JoinBuildLocal::push_keyed`], never this plain-push path.
    fn push_single_pass(&mut self, hashvalue: u32, payload: &[u8]) -> Result<u64, BudgetExceeded> {
        debug_assert!(self.dense_keys.is_none(), "dense seat unsupported under single-pass build");
        let (chunk_idx, off) = self.materialize(hashvalue, payload)?;
        let dir = self.shared_dir.clone().expect("single-pass push without a shared dir");
        let r = pack_ref(self.ordinal, chunk_idx, off);
        // SAFETY: `chunk.atomic(off)` views this tuple's next word (W0) as an
        // AtomicU64 — the module's chunk-atomic-view contract.
        let next_word = self.chunks[chunk_idx].atomic(off);
        dir.insert(r, next_word, hashvalue);
        self.tuples += 1;
        Ok(r)
    }

    pub fn tuples(&self) -> u64 {
        self.tuples
    }
}

// ---------------------------------------------------------------------------
// CombinePlan (§4): SEAL output → partition-parallel combine, over BORROWED
// Locals — the ParallelSink contract hands combine/finalize `&[Local]`, so
// the plan never owns the Locals; the frozen table adopts their chunk Arcs.
// ---------------------------------------------------------------------------

pub struct CombinePlan {
    /// ordinal → dense index into the sealed Locals slice (u16::MAX = absent).
    by_ordinal: Box<[u16]>,
    /// All runs across all Locals, ascending by range_start — the
    /// reproducible combine order (a debugging nicety since the 2026-07-13
    /// order directive; NOT a correctness contract).
    run_order: Vec<(u64, u32, u32)>, // (range_start, local, run)
    /// Two-pass (or grown single-pass) directory. Empty when `singledir` owns
    /// the buckets (single-pass, un-grown).
    buckets: Box<[AtomicU64]>,
    /// SINGLE-PASS (Phase 1a): the directory the build workers CAS-inserted
    /// into. When present the chains are already linked (`combine_partition`
    /// is never called) and bucket reads route here via [`bucket_slice`].
    singledir: Option<Arc<SharedBuildDir>>,
    log2_nbuckets: u32,
    total_tuples: u64,
}

impl CombinePlan {
    /// SEAL (single-threaded, or first-combine lazy init under the caller's
    /// lock): size the table from the TRUE tuple count (no constraint on
    /// nbuckets — §4), charge the bucket array to the envelope, order the
    /// runs. Every later call must pass the SAME `locals` slice (the sink
    /// plumbing's sealed Arc guarantees it).
    pub fn plan(locals: &[JoinBuildLocal], budget: &JoinBudget) -> Result<CombinePlan, BudgetExceeded> {
        // Sized to the max PRESENT ordinal, not MAX_ORDINALS (32768):
        // sparse high worker indices (192-core pin-board lanes) cost
        // 2 bytes/index, dense probing stays an array load.
        let slots = locals.iter().map(|l| l.ordinal as usize + 1).max().unwrap_or(0);
        assert!(locals.len() < u16::MAX as usize, "Local count exceeds dense-index space");
        let mut by_ordinal = vec![u16::MAX; slots].into_boxed_slice();
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
        Ok(CombinePlan {
            by_ordinal,
            run_order,
            buckets: buckets.into_boxed_slice(),
            singledir: None,
            log2_nbuckets: nbuckets.trailing_zeros(),
            total_tuples: total,
        })
    }

    /// The live bucket directory — the single-pass `SharedBuildDir`'s array
    /// when it owns the table, else this plan's own `buckets`.
    #[inline]
    fn bucket_slice(&self) -> &[AtomicU64] {
        match &self.singledir {
            Some(d) => &d.buckets,
            None => &self.buckets,
        }
    }

    pub fn partitions(&self) -> u64 {
        PARTITIONS as u64
    }

    pub fn total_tuples(&self) -> u64 {
        self.total_tuples
    }

    #[inline]
    fn chunk<'a>(&self, locals: &'a [JoinBuildLocal], r: u64) -> (&'a Chunk, usize) {
        let (ord, ci, off) = unpack_ref(r);
        let li = self.by_ordinal[ord];
        debug_assert!(li != u16::MAX, "ref to unknown Local ordinal");
        (&locals[li as usize].chunks[ci], off)
    }

    /// Build partition `part`'s bucket range: walk runs in ascending
    /// range order, within a run in materialization order, head-insert.
    /// EXACTLY-ONCE per partition, single writer for the partition's
    /// buckets and its tuples' `next` words (the ParallelSink combine
    /// contract) — hence plain relaxed stores.
    pub fn combine_partition(&self, part: u64, locals: &[JoinBuildLocal]) {
        let part = part as usize;
        assert!(part < PARTITIONS);
        for &(_, li, ri) in &self.run_order {
            let l = &locals[li as usize];
            let refs = &l.part_refs[part];
            let ri = ri as usize;
            let start = if ri == 0 { 0 } else { l.runs[ri - 1].ends[part] as usize };
            let end = l.runs[ri].ends[part] as usize;
            for &r in &refs[start..end] {
                let (chunk, off) = self.chunk(locals, r);
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
}

// ---------------------------------------------------------------------------
// SINGLE-PASS atomic-CAS build (Phase 1a; gather-elimination-plan §1a,
// research-parallelize-hashjoin §3.2 option A). One shared bucket directory,
// sized UP FRONT from the planner's inner-rows estimate; every build tuple is
// linked into its bucket chain ONCE, during the build scan, via an atomic CAS
// on the bucket head (a Treiber push) — killing the two-pass COMBINE re-read
// that loses 1.14–1.50× vs PG Parallel Hash / Umbra above ~2M rows. The 16-bit
// tag word (an embedded Bloom filter, §1.6) is maintained under the same CAS,
// so the probe pre-filter is identical to the two-pass table.
//
// CONTENTION NOTE (coordinator steer): the CAS contends on bucket HEADS. Low
// distinct-key / skewed builds pile many tuples onto a few chains → hot-line
// ping-pong that can erase (or invert) the bandwidth win. This is a BANDWIDTH
// optimization; the per-shape route_to flip stays fleet-gated precisely so the
// low-distinct/skew crossover is measured, not assumed. The CAS carries a
// spin-loop backoff on contention; correctness is contention-independent (a
// degenerate all-one-bucket build is covered by tests).
// ---------------------------------------------------------------------------

const GROW_LOAD_FACTOR: u64 = 2;

/// Link `packed_ref`'s tuple at `bucket`'s chain head. `next_word` is the
/// tuple's OWN W0 (this worker is its only writer); on a CAS race we re-point
/// it at the fresh head and retry. Lost-update freedom is the CAS itself,
/// independent of memory ordering; the Release success ordering publishes the
/// `next_word` store to any later acquirer of this bucket. LOOM-COVERED — the
/// `singlepass_loom` model mirrors this loop verbatim against loom atomics.
#[inline]
fn cas_insert_head(bucket: &AtomicU64, next_word: &AtomicU64, packed_ref: u64, tag: u64) {
    let mut old = bucket.load(Ordering::Relaxed);
    loop {
        // next := old head's packed ref+1 (0 when the bucket was empty).
        next_word.store(old >> 16, Ordering::Relaxed);
        let newv = ((packed_ref + 1) << 16) | ((old & 0xFFFF) | tag);
        match bucket.compare_exchange_weak(old, newv, Ordering::Release, Ordering::Relaxed) {
            Ok(_) => return,
            Err(cur) => {
                old = cur;
                std::hint::spin_loop(); // bounded backoff on hot-bucket contention
            }
        }
    }
}

/// The shared bucket directory for the single-pass build. Fixed size during
/// the concurrent accept phase (online mid-build resize is deferred — the
/// hardest PG code, §5 risk); underestimates are absorbed by a ONE-SHOT
/// barrier-gated `grow_buckets` at seal (`finish_single_pass`, single-threaded).
pub struct SharedBuildDir {
    buckets: Box<[AtomicU64]>,
    log2_nbuckets: u32,
    inserted: AtomicU64,
}

impl SharedBuildDir {
    /// Size from the planner's inner-rows estimate (rounded to a power of two,
    /// clamped to the same [`MIN_NBUCKETS`]..[`MAX_NBUCKETS`] band the
    /// two-pass plan uses) and charge the array to the shared envelope. A
    /// crossing BACKS THE CHARGE OUT and returns `BudgetExceeded` — the
    /// caller falls back to two-pass against the SAME (unpoisoned) budget,
    /// so single-pass alone never causes a refusal. The back-out is what
    /// makes that true: a multi-MB charge left in place would eat the whole
    /// envelope and turn the fallback's first chunk charge into a refusal.
    pub fn with_estimate(est_rows: u64, budget: &JoinBudget) -> Result<Arc<SharedBuildDir>, BudgetExceeded> {
        let nbuckets = est_rows.next_power_of_two().clamp(MIN_NBUCKETS, MAX_NBUCKETS);
        if !budget.try_charge_optional(nbuckets as usize * 8) {
            return Err(BudgetExceeded);
        }
        let buckets: Vec<AtomicU64> = (0..nbuckets).map(|_| AtomicU64::new(0)).collect();
        Ok(Arc::new(SharedBuildDir {
            buckets: buckets.into_boxed_slice(),
            log2_nbuckets: nbuckets.trailing_zeros(),
            inserted: AtomicU64::new(0),
        }))
    }

    /// CAS-link a materialized tuple. `next_word` = the tuple's W0 (its
    /// chunk's next word). Concurrent-safe across all build workers.
    #[inline]
    pub(crate) fn insert(&self, packed_ref: u64, next_word: &AtomicU64, hashvalue: u32) {
        let b = bucket_of(hashvalue, self.log2_nbuckets);
        cas_insert_head(&self.buckets[b], next_word, packed_ref, tag_bit(hashvalue));
        self.inserted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inserted(&self) -> u64 {
        self.inserted.load(Ordering::Relaxed)
    }

    pub fn nbuckets(&self) -> usize {
        self.buckets.len()
    }
}

/// SEAL for a single-pass build (single-threaded, at the accept→probe
/// barrier): optionally `grow_buckets` if the estimate underran the true
/// count, then wrap the directory as a [`CombinePlan`] the frozen table and
/// probe/fill paths consume UNCHANGED. `combine_partition` is never called on
/// the result — the chains are already linked. Consumes `dir`.
///
/// The TWO-PASS dense seat is always absent here (single-pass Locals never
/// arm `dense_keys`); when the SE-MBSEAT order-free pairs were tracked,
/// `freeze` builds the single-pass seat instead ([`build_seat_single_pass`]).
pub fn finish_single_pass(
    locals: &[JoinBuildLocal],
    dir: Arc<SharedBuildDir>,
    budget: &JoinBudget,
) -> Result<CombinePlan, BudgetExceeded> {
    // by_ordinal (dense index into the sealed Locals) — identical to `plan`.
    let slots = locals.iter().map(|l| l.ordinal as usize + 1).max().unwrap_or(0);
    assert!(locals.len() < u16::MAX as usize, "Local count exceeds dense-index space");
    let mut by_ordinal = vec![u16::MAX; slots].into_boxed_slice();
    for (li, l) in locals.iter().enumerate() {
        assert!(!l.in_run, "sealed a Local with an open run");
        assert!(l.shared_dir.is_some() || l.tuples == 0, "two-pass Local in a single-pass seal");
        assert!(
            by_ordinal[l.ordinal as usize] == u16::MAX,
            "duplicate Local ordinal {}",
            l.ordinal
        );
        by_ordinal[l.ordinal as usize] = li as u16;
    }

    // The Locals still hold Arc clones of `dir` at seal (the ParallelSink
    // hands finalize `&[Local]`), so we do NOT reclaim ownership; the plan
    // keeps its own Arc and reads buckets through it. The array is frozen and
    // immutable from here (probe reads only).
    let total = dir.inserted.load(Ordering::Relaxed);

    // Barrier-gated grow_buckets: if the up-front estimate underran the true
    // count past the load-factor bound, rehash ONCE into a right-sized array.
    // This reintroduces a single build-side pass for MIS-ESTIMATED builds
    // only (the documented §5 mitigation); a good estimate pays nothing.
    let want = total.next_power_of_two().clamp(MIN_NBUCKETS, MAX_NBUCKETS);
    if want > dir.buckets.len() as u64 && total > dir.buckets.len() as u64 * GROW_LOAD_FACTOR {
        let (buckets, log2_nbuckets) =
            grow_buckets(&dir.buckets, dir.log2_nbuckets, want, locals, &by_ordinal, budget)?;
        Ok(CombinePlan {
            by_ordinal,
            run_order: Vec::new(),
            buckets,
            singledir: None, // the grown array is plan-owned
            log2_nbuckets,
            total_tuples: total,
        })
    } else {
        let log2_nbuckets = dir.log2_nbuckets;
        Ok(CombinePlan {
            by_ordinal,
            run_order: Vec::new(), // single-pass: no COMBINE walk (seat, if any, is the order-free SE-MBSEAT build)
            buckets: Box::new([]),
            singledir: Some(dir),
            log2_nbuckets,
            total_tuples: total,
        })
    }
}

/// Rehash every tuple of the (frozen, single-threaded) old directory into a
/// larger array. Walks the OLD chains — the only tuple index a single-pass
/// build keeps — re-linking at the new heads with plain stores (no other
/// thread is live at seal). Preserves the full multiset; chain order within a
/// bucket changes, which is tie-normalized-OK (the 2026-07-13 order directive).
fn grow_buckets(
    old: &[AtomicU64],
    old_log2: u32,
    new_nbuckets: u64,
    locals: &[JoinBuildLocal],
    by_ordinal: &[u16],
    budget: &JoinBudget,
) -> Result<(Box<[AtomicU64]>, u32), BudgetExceeded> {
    let _ = old_log2;
    if !budget.try_charge(new_nbuckets as usize * 8) {
        return Err(BudgetExceeded);
    }
    // The grown array REPLACES the estimate-sized one: release the old
    // array's charge so the envelope holds ONE directory, not two. (The old
    // storage itself dies when the last `SharedBuildDir` Arc drops with the
    // Locals right after seal; the transient both-charged window above is
    // exactly the both-alive rehash window.)
    budget.release(old.len() * 8);
    let new_log2 = new_nbuckets.trailing_zeros();
    let newb: Vec<AtomicU64> = (0..new_nbuckets).map(|_| AtomicU64::new(0)).collect();
    let newb = newb.into_boxed_slice();
    let resolve = |r: u64| -> (&Chunk, usize) {
        let (ord, ci, off) = unpack_ref(r);
        let li = by_ordinal[ord];
        debug_assert!(li != u16::MAX, "grow: ref to unknown Local ordinal");
        (&locals[li as usize].chunks[ci], off)
    };
    for slot in old.iter() {
        let mut cur = slot.load(Ordering::Relaxed) >> 16; // ref+1
        while cur != 0 {
            let r = cur - 1;
            let (chunk, off) = resolve(r);
            let old_next = chunk.atomic(off).load(Ordering::Relaxed); // snapshot BEFORE relink
            let h = chunk.read(off + 1) as u32;
            let b = bucket_of(h, new_log2);
            let head = newb[b].load(Ordering::Relaxed);
            chunk.atomic(off).store(head >> 16, Ordering::Relaxed);
            newb[b].store(((r + 1) << 16) | ((head & 0xFFFF) | tag_bit(h)), Ordering::Relaxed);
            cur = old_next;
        }
    }
    Ok((newb, new_log2))
}

// ---------------------------------------------------------------------------
// HJPROBE-V2 dense seat (notes/se-hjprobe-v2.md §4.3 increment 1): the
// legacy nodehash DenseTable's direct-address idea on the frozen table, as
// CSR per-key candidate lists — key k's candidates are CONTIGUOUS packed
// refs in EXACTLY the v1 bucket-chain candidate order for that key (the
// same-key subsequence of the head-inserted chain = reverse insertion
// order; the legacy order-parity proof verbatim). The seat probe therefore
// emits byte-identically to the v1 walk while skipping the probe hash
// eval, the bucket/tag lookup, every hashvalue compare, and the
// hashclauses key recheck (int4 key equality == seat identity).
// ---------------------------------------------------------------------------

pub struct DenseSeat {
    min: i32,
    /// CSR offsets: key k's refs are `refs[offs[k-min] .. offs[k-min+1]]`.
    offs: Box<[u32]>,
    /// Packed tuple refs (NOT +1 encoded), per-key chain order.
    refs: Box<[u64]>,
}

impl DenseSeat {
    /// Key k's candidates in v1 probe emission order; out-of-range keys
    /// (and every key when the seat never built) answer the empty slice.
    #[inline(always)]
    pub(crate) fn candidates(&self, key: i32) -> &[u64] {
        let off = key as i64 - self.min as i64;
        if (off as u64) < (self.offs.len() as u64 - 1) {
            let o = off as usize;
            &self.refs[self.offs[o] as usize..self.offs[o + 1] as usize]
        } else {
            &[]
        }
    }
}

/// Seat construction at freeze (single-threaded — the finalize caller).
/// All-or-nothing: every tuple-bearing Local must be armed (a Local that
/// accepted zero morsels never armed and contributes nothing). Range and
/// budget gates mirror the legacy seat_dense laws (range ≤ 4×rows; the
/// arrays charge the envelope OPTIONALLY — a crossing forgoes the seat,
/// never refuses the build).
fn build_seat(plan: &CombinePlan, locals: &[JoinBuildLocal]) -> Option<DenseSeat> {
    let bearing: Vec<&JoinBuildLocal> = locals.iter().filter(|l| l.tuples > 0).collect();
    if bearing.is_empty() || !bearing.iter().all(|l| l.dense_keys.is_some()) {
        debug_assert!(
            bearing.iter().all(|l| l.dense_keys.is_none())
                || bearing.iter().all(|l| l.dense_keys.is_some()),
            "mixed dense-key arming across tuple-bearing Locals"
        );
        return None;
    }
    // Pass 0: min/max/any over non-NULL keys (order-free).
    let (mut min, mut max, mut seated) = (i32::MAX, i32::MIN, 0u64);
    for l in &bearing {
        for keys in &l.dense_keys.as_ref().expect("armed").part_keys {
            for &k in keys {
                if k == NULL_KEY {
                    continue;
                }
                let k = k as i32;
                min = min.min(k);
                max = max.max(k);
                seated += 1;
            }
        }
    }
    if seated == 0 {
        return None;
    }
    let range = max as i64 - min as i64 + 1;
    if range as u64 > seated.saturating_mul(4) {
        return None; // sparse keys: the seat would be mostly holes
    }
    let bytes = (range as usize + 1) * size_of::<u32>() + seated as usize * size_of::<u64>();
    if !bearing[0].budget.try_charge_optional(bytes) {
        return None;
    }
    // Pass 1: per-key counts -> exclusive prefix offs (offs[k+1] = end).
    let mut offs = vec![0u32; range as usize + 1].into_boxed_slice();
    for l in &bearing {
        for keys in &l.dense_keys.as_ref().expect("armed").part_keys {
            for &k in keys {
                if k != NULL_KEY {
                    offs[(k as i32 as i64 - min as i64) as usize + 1] += 1;
                }
            }
        }
    }
    for i in 1..offs.len() {
        offs[i] += offs[i - 1];
    }
    // Pass 2: fill each key's slice BACKWARD while walking the exact
    // combine enumeration (partition-major, runs ascending by range_start,
    // materialization order within a run) — the final slice order is
    // reverse insertion order = the head-inserted bucket chain's same-key
    // subsequence = the v1 probe's candidate order. `cursor` decrements
    // from each key's slice end.
    let mut cursor: Vec<u32> = offs[1..].to_vec(); // cursor[k] = slice end
    let mut refs = vec![0u64; seated as usize].into_boxed_slice();
    for part in 0..PARTITIONS {
        for &(_, li, ri) in &plan.run_order {
            let l = &locals[li as usize];
            let Some(dk) = l.dense_keys.as_ref() else { continue };
            let ri = ri as usize;
            let start = if ri == 0 { 0 } else { l.runs[ri - 1].ends[part] as usize };
            let end = l.runs[ri].ends[part] as usize;
            for idx in start..end {
                let k = dk.part_keys[part][idx];
                if k == NULL_KEY {
                    continue;
                }
                let slot = (k as i32 as i64 - min as i64) as usize;
                cursor[slot] -= 1;
                refs[cursor[slot] as usize] = l.part_refs[part][idx];
            }
        }
    }
    debug_assert!(cursor.iter().zip(offs.iter()).all(|(c, o)| c == o));
    Some(DenseSeat { min, offs, refs })
}

/// SE-MBSEAT: seat construction for SINGLE-PASS builds (single-threaded —
/// the freeze/seal barrier caller), from the Locals' order-free
/// `(packed_ref, key)` pairs. Same gates as [`build_seat`] (all-or-nothing
/// arming across tuple-bearing Locals, range ≤ 4x rows, OPTIONAL budget
/// charge — a crossing forgoes the seat, never refuses the build), but NO
/// chain-order reproduction: the CSR slices carry each key's candidates in
/// plain enumeration order. That is deliberate and consumer-scoped — the
/// multibuild walk's emission is order-insensitive through the sink absorb
/// (the 2026-07-13 order directive's baseline), and the single-pass chains
/// this seat shadows are concurrent-nondeterministic already. The
/// single-join byte-parity seat stays [`build_seat`], two-pass only.
fn build_seat_single_pass(locals: &[JoinBuildLocal]) -> Option<DenseSeat> {
    let bearing: Vec<&JoinBuildLocal> = locals.iter().filter(|l| l.tuples > 0).collect();
    if bearing.is_empty() || !bearing.iter().all(|l| l.sp_keys.is_some()) {
        debug_assert!(
            bearing.iter().all(|l| l.sp_keys.is_none())
                || bearing.iter().all(|l| l.sp_keys.is_some()),
            "mixed single-pass key arming across tuple-bearing Locals"
        );
        return None;
    }
    // Pass 0: min/max/count over non-NULL keys (order-free).
    let (mut min, mut max, mut seated) = (i32::MAX, i32::MIN, 0u64);
    for l in &bearing {
        for &(_, k) in l.sp_keys.as_ref().expect("armed") {
            if k == NULL_KEY {
                continue;
            }
            let k = k as i32;
            min = min.min(k);
            max = max.max(k);
            seated += 1;
        }
    }
    if seated == 0 {
        return None;
    }
    let range = max as i64 - min as i64 + 1;
    if range as u64 > seated.saturating_mul(4) {
        return None; // sparse keys: the seat would be mostly holes
    }
    let bytes = (range as usize + 1) * size_of::<u32>() + seated as usize * size_of::<u64>();
    if !bearing[0].budget.try_charge_optional(bytes) {
        return None;
    }
    // Pass 1: per-key counts -> exclusive prefix offs (offs[k+1] = end).
    let mut offs = vec![0u32; range as usize + 1].into_boxed_slice();
    for l in &bearing {
        for &(_, k) in l.sp_keys.as_ref().expect("armed") {
            if k != NULL_KEY {
                offs[(k as i32 as i64 - min as i64) as usize + 1] += 1;
            }
        }
    }
    for i in 1..offs.len() {
        offs[i] += offs[i - 1];
    }
    // Pass 2: fill FORWARD in enumeration order (order-free; see above).
    let mut cursor: Vec<u32> = offs[..offs.len() - 1].to_vec(); // cursor[k] = slice start
    let mut refs = vec![0u64; seated as usize].into_boxed_slice();
    for l in &bearing {
        for &(r, k) in l.sp_keys.as_ref().expect("armed") {
            if k == NULL_KEY {
                continue;
            }
            let slot = (k as i32 as i64 - min as i64) as usize;
            refs[cursor[slot] as usize] = r;
            cursor[slot] += 1;
        }
    }
    debug_assert!(cursor.iter().zip(offs[1..].iter()).all(|(c, o)| c == o));
    Some(DenseSeat { min, offs, refs })
}

/// Publish (§4 finalize): freeze — adopt the Locals' chunk Arcs (dense
/// order, matching `by_ordinal`) and the finished plan. The Locals then
/// drop with the sink plumbing; the storage survives in the table. When
/// the Locals tracked dense keys (HJPROBE-V2 two-pass, or the SE-MBSEAT
/// single-pass pairs), the seat builds here — or silently doesn't
/// (range/budget gates), leaving the v1 probe.
pub fn freeze(plan: Arc<CombinePlan>, locals: &[JoinBuildLocal]) -> FrozenJoinTable {
    let chunk_lists: Vec<Box<[Arc<Chunk>]>> =
        locals.iter().map(|l| l.chunks.clone().into_boxed_slice()).collect();
    let seat = build_seat(&plan, locals).or_else(|| build_seat_single_pass(locals));
    FrozenJoinTable { plan, chunk_lists, seat }
}

// ---------------------------------------------------------------------------
// FrozenJoinTable (§4/§5): the probe/fill face.
// ---------------------------------------------------------------------------

pub struct FrozenJoinTable {
    plan: Arc<CombinePlan>,
    /// Dense (by_ordinal order) adopted chunk storage.
    chunk_lists: Vec<Box<[Arc<Chunk>]>>,
    /// HJPROBE-V2 dense seat (None = v1 probe, always).
    seat: Option<DenseSeat>,
}

impl FrozenJoinTable {
    pub fn nbuckets(&self) -> usize {
        self.plan.bucket_slice().len()
    }

    /// HJPROBE-V2: the dense seat, when it built (knob-armed build + the
    /// range/budget gates passed). Its presence IS the probe dispatch.
    #[inline(always)]
    pub(crate) fn seat(&self) -> Option<&DenseSeat> {
        self.seat.as_ref()
    }

    /// Whether the dense probe will engage (the runtime's dispatch probe).
    #[inline(always)]
    pub fn has_seat(&self) -> bool {
        self.seat.is_some()
    }

    /// A [`TupleRef`] view of a packed ref OBTAINED FROM THIS TABLE's seat
    /// or chains (the seat probe's candidate materializer).
    #[inline(always)]
    pub(crate) fn tuple_ref(&self, r: u64) -> TupleRef<'_> {
        TupleRef { table: self, r }
    }

    pub fn total_tuples(&self) -> u64 {
        self.plan.total_tuples
    }

    #[inline]
    fn chunk(&self, r: u64) -> (&Chunk, usize) {
        let (ord, ci, off) = unpack_ref(r);
        (&self.chunk_lists[self.plan.by_ordinal[ord] as usize][ci], off)
    }

    /// The probe entry: the hash's bucket chain, tag-prefiltered (a tag
    /// miss returns an empty iterator after ONE bucket-word read).
    /// Yields every chain tuple; the caller filters by hashvalue + quals
    /// (C's probe discipline).
    pub fn chain(&self, hashvalue: u32) -> ChainIter<'_> {
        let word = self.plan.bucket_slice()[bucket_of(hashvalue, self.plan.log2_nbuckets)]
            .load(Ordering::Relaxed);
        let head = if word & tag_bit(hashvalue) != 0 { word >> 16 } else { 0 };
        ChainIter { table: self, next_packed: head }
    }

    /// Unfiltered bucket walk (fill phase + tests).
    pub fn bucket_chain(&self, bucket: usize) -> ChainIter<'_> {
        ChainIter {
            table: self,
            next_packed: self.plan.bucket_slice()[bucket].load(Ordering::Relaxed) >> 16,
        }
    }

    /// Partition `part`'s exclusive bucket range (§4 layout).
    pub fn partition_buckets(&self, part: u64) -> Range<usize> {
        let per = self.plan.bucket_slice().len() / PARTITIONS;
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
    ) -> Result<Vec<JoinBuildLocal>, BudgetExceeded> {
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
        Ok(locals)
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

    fn combine_all_serial(plan: &CombinePlan, locals: &[JoinBuildLocal]) {
        for p in 0..PARTITIONS as u64 {
            plan.combine_partition(p, locals);
        }
    }

    fn combine_all_parallel(plan: &CombinePlan, locals: &[JoinBuildLocal], threads: usize) {
        let next = AtomicU64::new(0);
        std::thread::scope(|scope| {
            for _ in 0..threads {
                scope.spawn(|| loop {
                    let p = next.fetch_add(1, Ordering::Relaxed);
                    if p >= PARTITIONS as u64 {
                        break;
                    }
                    plan.combine_partition(p, locals);
                });
            }
        });
    }

    /// plan + combine (serial or parallel) + freeze, dropping the Locals —
    /// the sink plumbing's exact lifecycle.
    fn plan_combine_freeze(
        locals: Vec<JoinBuildLocal>,
        budget: &JoinBudget,
        parallel: bool,
    ) -> FrozenJoinTable {
        let plan = Arc::new(CombinePlan::plan(&locals, budget).expect("plan within budget"));
        if parallel {
            combine_all_parallel(&plan, &locals, 8);
        } else {
            combine_all_serial(&plan, &locals);
        }
        let t = freeze(Arc::clone(&plan), &locals);
        drop(locals); // storage must survive the Locals (Arc adoption)
        t
    }

    fn assert_serial_identical(ds: &Dataset, schedule: &Schedule, parallel_combine: bool) {
        let budget = JoinBudget::unlimited();
        let locals = build_from_schedule(ds, schedule, &budget).expect("unlimited budget");
        let t = plan_combine_freeze(locals, &budget, parallel_combine);
        let l2 = (t.nbuckets() as u64).trailing_zeros();
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
        let t = plan_combine_freeze(Vec::new(), &budget, false);
        assert_eq!(t.total_tuples(), 0);
        assert!(frozen_chains(&t).iter().all(|c| c.is_empty()));
        assert_eq!(t.chain(0xDEAD_BEEF).count(), 0);

        // Locals that forked but saw only empty runs.
        let mut l = JoinBuildLocal::new(3, Arc::clone(&budget));
        l.begin_run(10);
        l.end_run();
        let t = plan_combine_freeze(vec![l], &budget, false);
        assert_eq!(t.total_tuples(), 0);
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
        let t = plan_combine_freeze(vec![with_empties], &budget, false);
        let l2 = (t.nbuckets() as u64).trailing_zeros();
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
        let t = plan_combine_freeze(vec![a, b], &budget, false);
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
        let locals = build_from_schedule(&ds, &vec![vec![0..16]], &budget).unwrap();
        let t = plan_combine_freeze(locals, &budget, false);
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
        let t = plan_combine_freeze(vec![l], &budget, false);
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
        let t = plan_combine_freeze(vec![l], &budget, false);
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
        // Fits during accept (one 64KB chunk + 700 ref-words ≈ 71.1KB
        // against a 72KB limit), crossed by the 1024-bucket array (8KB)
        // at SEAL.
        let budget = JoinBudget::new(CHUNK_MIN_WORDS * 8 + 8 * 1024);
        let mut l = JoinBuildLocal::new(0, Arc::clone(&budget));
        l.begin_run(0);
        for i in 0..700u64 {
            l.push(mix(i) as u32, &i.to_le_bytes()).unwrap();
        }
        l.end_run();
        assert_eq!(CombinePlan::plan(&[l], &budget).err(), Some(BudgetExceeded));
    }

    // ---- match flags (§5; loom-adjacent stress — the real barrier is
    // the runtime's Loom-verified task-set completion) ----

    #[test]
    fn match_flags_concurrent_probe_then_fill_exact_set() {
        let ds = Dataset { granules: 32, rows_per_granule: 16, key_space: 64, seed: 99, force_partition: None };
        let budget = JoinBudget::unlimited();
        let locals = build_from_schedule(&ds, &deal(ranges_of_sizes(32, std::iter::repeat(3)), 4, 5), &budget).unwrap();
        let t = plan_combine_freeze(locals, &budget, true);

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
        let t = plan_combine_freeze(vec![l], &budget, false);
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

    // ---- M3.5 batch-spill hooks: drain/reset + chunk cap ----

    #[test]
    fn drain_records_roundtrip_and_reset() {
        let ds = ds_default();
        let budget = JoinBudget::unlimited();
        let mut l = JoinBuildLocal::with_chunk_cap(0, Arc::clone(&budget), CHUNK_MIN_WORDS);
        l.begin_run(0);
        for g in 0..ds.granules {
            for (h, p) in ds.rows_of(g) {
                l.push(h, &p).unwrap();
            }
        }
        l.end_run();
        assert!(l.chunks.len() > 1, "chunk cap must bound growth");
        let mut drained: Vec<(u32, Vec<u8>)> = Vec::new();
        l.drain_records(|h, p| -> Result<(), ()> {
            drained.push((h, p.to_vec()));
            Ok(())
        })
        .unwrap();
        let mut expect = ds.all_rows();
        drained.sort();
        expect.sort();
        assert_eq!(drained, expect, "drain must yield the exact pushed multiset");
        l.reset();
        assert_eq!(l.tuples(), 0);
        assert!(l.chunks.is_empty());
        let mut n = 0;
        l.drain_records(|_, _| -> Result<(), ()> {
            n += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(n, 0, "reset Local drains nothing");
        // The Local is reusable post-reset.
        l.begin_run(7);
        l.push(0xAB, b"post-reset").unwrap();
        l.end_run();
        assert_eq!(l.tuples(), 1);
    }

    // ---- DOP-192 readiness: high worker ordinals (absolute runtime
    // worker indices — the old 8-bit ref field asserted at 256, which an
    // absolute index crosses at nthreads ≥ 193 + pin-board lanes) ----

    #[test]
    fn pack_ref_roundtrip_at_field_extremes() {
        // Max realistic offset: a tuple can start no later than the last
        // word of a CHUNK_MAX_WORDS chunk (over-`need` chunks hold one
        // tuple at offset 0), so CHUNK_MAX_WORDS - 1 bounds it.
        for &(ord, ci, off) in &[
            (0u16, 0usize, 0usize),
            (191, 3, 17),
            (255, 255, CHUNK_MAX_WORDS - 1), // old layout's ordinal max
            (256, 0, 0),                     // first index past the old assert
            ((MAX_ORDINALS - 1) as u16, MAX_CHUNKS_PER_LOCAL - 1, CHUNK_MAX_WORDS - 1),
        ] {
            let r = pack_ref(ord, ci, off);
            assert!(r < 1 << 48, "ref must stay in the bucket word's 48 bits");
            assert_eq!(unpack_ref(r), (ord as usize, ci, off));
        }
    }

    #[test]
    fn high_worker_ordinals_build_identically() {
        // Sparse ABSOLUTE worker indices as the 192-core runtime would
        // hand fork(): dop-192 body, pin-board lane, old-assert boundary,
        // ref-field max. Chains must still match the serial oracle.
        let ds = ds_default();
        let ordinals = [0usize, 191, 255, 256, 300, MAX_ORDINALS - 1];
        let budget = JoinBudget::unlimited();
        let ranges = ranges_of_sizes(ds.granules, std::iter::repeat(3));
        let mut locals: Vec<JoinBuildLocal> = ordinals
            .iter()
            .map(|&o| JoinBuildLocal::new(o, Arc::clone(&budget)))
            .collect();
        for (i, range) in ranges.into_iter().enumerate() {
            let l = &mut locals[i % ordinals.len()];
            l.begin_run(range.start);
            for g in range {
                for (h, p) in ds.rows_of(g) {
                    l.push(h, &p).unwrap();
                }
            }
            l.end_run();
        }
        let t = plan_combine_freeze(locals, &budget, true);
        let l2 = (t.nbuckets() as u64).trailing_zeros();
        assert_eq!(t.total_tuples(), ds.granules * ds.rows_per_granule);
        assert_eq!(frozen_chains(&t), reference_chains(&ds.all_rows(), l2));
    }

    // ---- HJPROBE-V2 dense seat (notes/se-hjprobe-v2.md §4.3 inc 1) ----
    //
    // THE CHAIN-ORDER PIN: for every key, the seat's CSR candidate slice
    // must equal the v1 probe's candidate sequence (the tag-prefiltered
    // bucket-chain walk filtered by hashvalue + key) EXACTLY, element for
    // element, over adversarial claim schedules and parallel combines —
    // the byte-identity argument for skipping the recheck rides on it.

    /// Keyed dataset row stream: key ∈ 0..key_space, hash = f(key) via the
    /// engine-shaped mix (same key ⇒ same hash, always), payload embeds
    /// the key + global row id so sequences compare exactly.
    fn keyed_rows_of(seed: u64, key_space: u64, rows_per_granule: u64, g: u64) -> Vec<(i32, u32, Vec<u8>)> {
        (0..rows_per_granule)
            .map(|i| {
                let id = g * rows_per_granule + i;
                let key = (mix(seed ^ id) % key_space) as i32;
                let h = mix((key as u64).wrapping_mul(0x517c_c1b7_2722_0a95)) as u32;
                let mut payload = (key as i64).to_le_bytes().to_vec();
                payload.extend(id.to_le_bytes());
                (key, h, payload)
            })
            .collect()
    }

    fn build_keyed_from_schedule(
        seed: u64,
        key_space: u64,
        rows_per_granule: u64,
        schedule: &Schedule,
        budget: &Arc<JoinBudget>,
        null_every: Option<u64>, // Some(n): every n-th row records NULL_KEY
    ) -> Vec<JoinBuildLocal> {
        let mut locals = Vec::new();
        for (w, claims) in schedule.iter().enumerate() {
            let mut l = JoinBuildLocal::new(w, Arc::clone(budget));
            l.arm_dense_keys();
            assert!(l.dense_armed());
            for range in claims {
                l.begin_run(range.start);
                for g in range.clone() {
                    for (key, h, p) in keyed_rows_of(seed, key_space, rows_per_granule, g) {
                        let id = u64::from_le_bytes(p[8..16].try_into().unwrap());
                        let k = match null_every {
                            Some(n) if id % n == 0 => NULL_KEY,
                            _ => key as i64,
                        };
                        l.push_keyed(h, &p, k).unwrap();
                    }
                }
                l.end_run();
            }
            locals.push(l);
        }
        locals
    }

    /// The v1 probe's candidate sequence for `key`: the tag-prefiltered
    /// chain walk with the hashvalue prefilter + key recheck — exactly
    /// what probe_after_hash yields to the arms.
    fn v1_candidates(t: &FrozenJoinTable, key: i32) -> Vec<(u32, Vec<u8>)> {
        let h = mix((key as u64).wrapping_mul(0x517c_c1b7_2722_0a95)) as u32;
        t.chain(h)
            .filter(|c| c.hashvalue() == h)
            .filter(|c| i64::from_le_bytes(c.payload()[..8].try_into().unwrap()) == key as i64)
            .map(|c| (c.hashvalue(), c.payload().to_vec()))
            .collect()
    }

    fn seat_candidates(t: &FrozenJoinTable, key: i32) -> Vec<(u32, Vec<u8>)> {
        let seat = t.seat().expect("seat must be present");
        seat.candidates(key)
            .iter()
            .map(|&r| {
                let tr = t.tuple_ref(r);
                (tr.hashvalue(), tr.payload().to_vec())
            })
            .collect()
    }

    #[test]
    fn dense_seat_candidates_match_v1_chain_walk_exactly() {
        // Duplicate-heavy keys, adversarial schedules, parallel combine.
        let (seed, key_space, rpg, granules) = (0xDE5E, 97u64, 37u64, 64u64);
        for sched_seed in 0..8u64 {
            let sizes = (0..).map(|i| (mix(sched_seed ^ i) % 7) + 1);
            let sched = deal(
                ranges_of_sizes(granules, sizes.take(64)),
                1 + (sched_seed as usize % 5),
                sched_seed,
            );
            let budget = JoinBudget::unlimited();
            let locals =
                build_keyed_from_schedule(seed, key_space, rpg, &sched, &budget, None);
            let t = plan_combine_freeze(locals, &budget, sched_seed % 2 == 0);
            assert!(t.has_seat(), "dense keys over a dense range must seat");
            for key in 0..key_space as i32 {
                assert_eq!(
                    seat_candidates(&t, key),
                    v1_candidates(&t, key),
                    "seat order diverges from the v1 walk (key {key}, sched {sched_seed})"
                );
                assert!(!seat_candidates(&t, key).is_empty(), "every key occurs at this scale");
            }
            // Out-of-range probes answer empty (the v1 walk finds nothing).
            for key in [-1, key_space as i32, i32::MAX, i32::MIN] {
                assert!(t.seat().unwrap().candidates(key).is_empty());
                assert!(v1_candidates(&t, key).is_empty());
            }
        }
    }

    #[test]
    fn dense_seat_null_keys_stay_out_of_seat_but_in_chains() {
        let (seed, key_space, rpg, granules) = (0xA11u64, 50u64, 20u64, 32u64);
        let sched = deal(ranges_of_sizes(granules, std::iter::repeat(3)), 4, 7);
        let budget = JoinBudget::unlimited();
        // Every 5th row records NULL_KEY (SQL NULL build key).
        let locals =
            build_keyed_from_schedule(seed, key_space, rpg, &sched, &budget, Some(5));
        let t = plan_combine_freeze(locals, &budget, true);
        assert!(t.has_seat());
        let mut seated = 0usize;
        for key in 0..key_space as i32 {
            let sc = seat_candidates(&t, key);
            seated += sc.len();
            // Seat = the v1 candidates whose row id is NOT null-keyed.
            let v1_nonnull: Vec<(u32, Vec<u8>)> = v1_candidates(&t, key)
                .into_iter()
                .filter(|(_, p)| u64::from_le_bytes(p[8..16].try_into().unwrap()) % 5 != 0)
                .collect();
            assert_eq!(sc, v1_nonnull, "NULL-keyed rows must be absent, order intact");
        }
        let total = (granules * rpg) as usize;
        assert_eq!(seated, total - total.div_ceil(5), "exactly the non-NULL rows seat");
        assert_eq!(t.total_tuples(), total as u64, "chains keep every row (fill walk parity)");
    }

    #[test]
    fn dense_seat_refuses_sparse_range() {
        let budget = JoinBudget::unlimited();
        let mut l = JoinBuildLocal::new(0, Arc::clone(&budget));
        l.arm_dense_keys();
        l.begin_run(0);
        // Two keys a mile apart: range >> 4x rows.
        for (key, id) in [(0i64, 1u64), (50_000_000, 2)] {
            let h = mix((key as u64).wrapping_mul(0x517c_c1b7_2722_0a95)) as u32;
            let mut p = key.to_le_bytes().to_vec();
            p.extend(id.to_le_bytes());
            l.push_keyed(h, &p, key).unwrap();
        }
        l.end_run();
        let t = plan_combine_freeze(vec![l], &budget, false);
        assert!(!t.has_seat(), "sparse key range must not seat");
        // The v1 probe still answers.
        assert_eq!(v1_candidates(&t, 0).len(), 1);
    }

    #[test]
    fn dense_seat_budget_crossing_forgoes_seat_not_build() {
        // Enough for chunks + refs + buckets, NOT for the seat arrays.
        let (seed, key_space, rpg, granules) = (7u64, 40u64, 10u64, 16u64);
        let rows = granules * rpg; // 160 tuples, payload 16B -> ~1 chunk
        let need_build = CHUNK_MIN_WORDS * 8 + rows as usize * 8 + 1024 * 8;
        let budget = JoinBudget::new(need_build + 64); // seat won't fit
        let sched = vec![vec![0..granules]];
        let locals = build_keyed_from_schedule(seed, key_space, rpg, &sched, &budget, None);
        let t = plan_combine_freeze(locals, &budget, false);
        assert!(!t.has_seat(), "seat must yield to the envelope");
        let total: usize = (0..key_space as i32).map(|k| v1_candidates(&t, k).len()).sum();
        assert_eq!(total, rows as usize, "build survives seat refusal — every row probeable");
    }

    #[test]
    fn dense_seat_ignores_empty_unarmed_locals() {
        // An armed bearing Local + an UNARMED EMPTY Local: the empty one
        // must not veto the seat (all-or-none applies to tuple-bearing).
        let budget = JoinBudget::unlimited();
        let mut a = JoinBuildLocal::new(0, Arc::clone(&budget));
        a.arm_dense_keys();
        a.begin_run(0);
        for (key, id) in [(3i64, 1u64), (4, 2), (3, 3)] {
            let h = mix((key as u64).wrapping_mul(0x517c_c1b7_2722_0a95)) as u32;
            let mut p = key.to_le_bytes().to_vec();
            p.extend(id.to_le_bytes());
            a.push_keyed(h, &p, key).unwrap();
        }
        a.end_run();
        let empty = JoinBuildLocal::new(1, Arc::clone(&budget)); // never armed, no tuples
        let t = plan_combine_freeze(vec![a, empty], &budget, false);
        assert!(t.has_seat(), "an empty unarmed Local must not veto the seat");
        assert_eq!(seat_candidates(&t, 3), v1_candidates(&t, 3));
        assert_eq!(seat_candidates(&t, 3).len(), 2);
        assert_eq!(seat_candidates(&t, 4).len(), 1);
    }

    // ---- HJPROBE-V2 probe-economics census (notes/se-hjprobe-v2.md §4) ----
    //
    // Replays the K2 letter corpora's EXACT key distributions
    // (corpus-k2win-q13/q18: deterministic multiplicative-hash mixes, no
    // random()) against a real FrozenJoinTable using the engine's real
    // int4 join hash (hashfn::hash_bytes_uint32 — the nodehash build-hash
    // kernel's function), and counts what the probe walk actually does:
    //
    //   probes        : outer rows probed
    //   walk_entered  : probes whose tag-prefiltered chain yielded >=1
    //                   candidate (the complement was killed by ONE
    //                   bucket-word read — empty bucket or tag miss)
    //   candidates    : chain tuples yielded across all probes
    //   hash_eq       : candidates surviving the hashvalue prefilter
    //   matches       : candidates whose KEY equals the probe key (what
    //                   the hashclauses exec_qual recheck would pass)
    //
    // The dead-probe rows adjudicate the bloom increment: a Bloom filter
    // consulted before the bucket lookup can only save work on probes the
    // tag word did NOT already kill. The candidate/match ratios bound the
    // dense-seat increment's chain-walk savings. Numbers are pinned
    // exactly (everything is deterministic); the full-scale twins are
    // #[ignore]d census runs whose numbers ride the worklog.

    struct ProbeCensus {
        probes: u64,
        walk_entered: u64,
        candidates: u64,
        hash_eq: u64,
        matches: u64,
    }

    fn probe_census(
        build_keys: impl Iterator<Item = i32>,
        probe_keys: impl Iterator<Item = i32>,
    ) -> ProbeCensus {
        let budget = JoinBudget::unlimited();
        let mut l = JoinBuildLocal::new(0, Arc::clone(&budget));
        l.begin_run(0);
        for k in build_keys {
            l.push(::hashfn::hash_bytes_uint32(k as u32), &k.to_le_bytes()).unwrap();
        }
        l.end_run();
        let t = plan_combine_freeze(vec![l], &budget, false);
        let mut c = ProbeCensus { probes: 0, walk_entered: 0, candidates: 0, hash_eq: 0, matches: 0 };
        for k in probe_keys {
            let h = ::hashfn::hash_bytes_uint32(k as u32);
            c.probes += 1;
            let mut any = false;
            for cand in t.chain(h) {
                any = true;
                c.candidates += 1;
                if cand.hashvalue() != h {
                    continue;
                }
                c.hash_eq += 1;
                if cand.payload() == k.to_le_bytes() {
                    c.matches += 1;
                }
            }
            if any {
                c.walk_entered += 1;
            }
        }
        c
    }

    /// corpus-k2win-q13's fact key stream (o_ckey): dead_mod'th rows point
    /// past the dim (the LEFT-join null tail), the rest spread over the
    /// low `live_span` dim keys. `dim_rows`/`dead_span` per the corpus.
    fn q13_probe_keys(
        fact_rows: i64,
        dim_rows: i64,
        live_span: i64,
        dead_span: i64,
        dead_mod: i64,
    ) -> impl Iterator<Item = i32> {
        (1..=fact_rows).map(move |g| {
            if g % dead_mod == 0 {
                (dim_rows + (g % dead_span) + 1) as i32
            } else {
                ((g * 48271) % live_span + 1) as i32
            }
        })
    }

    #[test]
    fn probe_census_q13_channel_scale() {
        // The CORPUS=hj hj13* fixture exactly (10k dim, 300k fact, 1/3
        // dead keys over 10001..15001, live spread 1..9000).
        let c = probe_census(1..=10_000i32, q13_probe_keys(300_000, 10_000, 9_000, 5_000, 3));
        assert_eq!(c.probes, 300_000);
        assert_eq!(c.matches, 200_000, "every live probe matches its unique dim row");
        // Pinned census (deterministic: real hash, fixed streams). The
        // adjudication ratios ride notes/se-hjprobe-v2.md §4:
        //   dead probes = 100_000; dead walks entered = walk_entered -
        //   live_walks; live probes always enter (their match is chained).
        let (dead, dead_walks) = (100_000u64, c.walk_entered - 200_000);
        assert_eq!(c.walk_entered, 203_540, "pinned: only 3.54% of dead probes survive tag+empty");
        assert_eq!(c.candidates, 327_180, "pinned: 1.09 candidates/probe — chains are short");
        assert_eq!(c.hash_eq, 200_000, "pinned: hashvalue prefilter rejects EVERY non-match candidate here");
        assert!(dead_walks * 20 < dead, "tag word + empty buckets eat >95% of dead probes");
    }

    #[test]
    fn probe_census_q18_channel_scale() {
        // The CORPUS=hj hj18* fixture exactly (125k orders build, 500k
        // lineitem probe, 1/50 dead keys, live spread 1..125000).
        let c = probe_census(
            1..=125_000i32,
            q13_probe_keys(500_000, 125_000, 125_000, 10_000, 50),
        );
        assert_eq!(c.probes, 500_000);
        assert_eq!(c.matches, 490_000);
        assert_eq!(c.walk_entered, 490_700, "pinned: dead-probe walks are 7.0% of dead probes");
        assert_eq!(c.candidates, 956_008, "pinned: 1.91 candidates/probe at 0.954 load factor");
        assert_eq!(c.hash_eq, 490_016, "pinned: 16 full-hash collisions — the exec_qual recheck earns its keep 16 times in 500k probes");
    }

    #[test]
    #[ignore = "letter-scale census (3M/5M probes): numbers ride notes/se-hjprobe-v2.md §4"]
    fn probe_census_q13_letter_scale() {
        let c = probe_census(1..=100_000i32, q13_probe_keys(3_000_000, 100_000, 90_000, 50_000, 3));
        eprintln!(
            "q13 letter census: probes={} walk_entered={} candidates={} hash_eq={} matches={}",
            c.probes, c.walk_entered, c.candidates, c.hash_eq, c.matches
        );
        assert_eq!(c.matches, 2_000_000);
    }

    #[test]
    #[ignore = "letter-scale census (5M probes): numbers ride notes/se-hjprobe-v2.md §4"]
    fn probe_census_q18_letter_scale() {
        let c = probe_census(
            1..=1_250_000i32,
            q13_probe_keys(5_000_000, 1_250_000, 1_250_000, 100_000, 50),
        );
        eprintln!(
            "q18 letter census: probes={} walk_entered={} candidates={} hash_eq={} matches={}",
            c.probes, c.walk_entered, c.candidates, c.hash_eq, c.matches
        );
        assert_eq!(c.matches, 4_900_000);
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

    // ---- SINGLE-PASS atomic-CAS build (Phase 1a) ----
    //
    // The single-pass build links every tuple concurrently at its bucket
    // head via CAS during accept — no COMBINE. Chain order within a bucket
    // is therefore NONDETERMINISTIC (many writers race the head), so the
    // oracle is TIE-NORMALIZED: the frozen table must hold the exact same
    // MULTISET of tuples as the serial reference, each in bucket_of(hash),
    // and every tuple must be reachable via chain() (no lost CAS updates).

    /// Build single-pass over `schedule` into a directory sized from
    /// `est_rows`, then seal+freeze. `concurrent` spawns one scoped thread
    /// per worker (the real racing insert); otherwise workers run serially
    /// (still exercises the CAS + seal path deterministically).
    fn build_single_pass(
        ds: &Dataset,
        schedule: &Schedule,
        est_rows: u64,
        budget: &Arc<JoinBudget>,
        concurrent: bool,
    ) -> FrozenJoinTable {
        let dir = SharedBuildDir::with_estimate(est_rows, budget).expect("dir within budget");
        let mut locals: Vec<JoinBuildLocal> = (0..schedule.len())
            .map(|w| {
                let mut l = JoinBuildLocal::new(w, Arc::clone(budget));
                l.attach_shared_dir(Arc::clone(&dir));
                l
            })
            .collect();
        let run_worker = |l: &mut JoinBuildLocal, claims: &Vec<Range<u64>>| {
            for range in claims {
                l.begin_run(range.start);
                for g in range.clone() {
                    for (h, p) in ds.rows_of(g) {
                        l.push(h, &p).unwrap();
                    }
                }
                l.end_run();
            }
        };
        if concurrent {
            std::thread::scope(|scope| {
                for (w, l) in locals.iter_mut().enumerate() {
                    let claims = &schedule[w];
                    scope.spawn(move || run_worker(l, claims));
                }
            });
        } else {
            for (w, l) in locals.iter_mut().enumerate() {
                run_worker(l, &schedule[w]);
            }
        }
        let plan = Arc::new(finish_single_pass(&locals, dir, budget).expect("finish within budget"));
        let t = freeze(Arc::clone(&plan), &locals);
        drop(locals); // chunk storage survives via Arc adoption
        t
    }

    /// Every tuple in the table as a sorted (bucket, hash, payload) multiset —
    /// also asserts each tuple sits in bucket_of(hash) (chain integrity).
    fn frozen_multiset(t: &FrozenJoinTable) -> Vec<(u32, Vec<u8>)> {
        let l2 = (t.nbuckets() as u64).trailing_zeros();
        let mut out = Vec::new();
        for b in 0..t.nbuckets() {
            for tr in t.bucket_chain(b) {
                let h = tr.hashvalue();
                assert_eq!(bucket_of(h, l2), b, "tuple in the wrong bucket");
                out.push((h, tr.payload().to_vec()));
            }
        }
        out.sort();
        out
    }

    fn sorted_rows(ds: &Dataset) -> Vec<(u32, Vec<u8>)> {
        let mut r = ds.all_rows();
        r.sort();
        r
    }

    /// Also assert chain() (the tag-prefiltered probe entry) reaches every
    /// tuple — the CAS must never lose a tuple nor hide it behind a stale tag.
    fn assert_single_pass_multiset(ds: &Dataset, schedule: &Schedule, est_rows: u64, concurrent: bool) {
        let budget = JoinBudget::unlimited();
        let t = build_single_pass(ds, schedule, est_rows, &budget, concurrent);
        assert_eq!(t.total_tuples(), ds.granules * ds.rows_per_granule);
        assert_eq!(frozen_multiset(&t), sorted_rows(ds), "single-pass multiset diverges");
        for (h, p) in ds.all_rows() {
            assert!(
                t.chain(h).any(|tr| tr.hashvalue() == h && tr.payload() == &p[..]),
                "single-pass lost a tuple on the probe path (hash {h:#x})"
            );
        }
    }

    #[test]
    fn single_pass_serial_matches_oracle_multiset() {
        let ds = ds_default();
        assert_single_pass_multiset(&ds, &vec![vec![0..ds.granules]], ds.granules * ds.rows_per_granule, false);
    }

    #[test]
    fn single_pass_concurrent_matches_oracle_multiset() {
        let ds = ds_default();
        for seed in 0..12u64 {
            let sizes = (0..).map(|i| (mix(seed ^ i) % 7) + 1);
            let sched = deal(ranges_of_sizes(ds.granules, sizes.take(64)), 1 + (seed as usize % 8), seed);
            // Estimate deliberately spot-on for these (grow untouched).
            assert_single_pass_multiset(&ds, &sched, ds.granules * ds.rows_per_granule, true);
        }
    }

    #[test]
    fn single_pass_hot_bucket_skew_is_correct() {
        // MAXIMAL contention: every tuple hashes into ONE partition and a
        // tiny key space ⇒ a few very hot chain heads. This is the shape the
        // coordinator flagged where the CAS ping-pongs; correctness must be
        // contention-INDEPENDENT even if throughput is not.
        let mut ds = ds_default();
        ds.force_partition = Some(0x7C);
        ds.key_space = 3; // 3 chains take all the traffic
        ds.rows_per_granule = 64;
        let sched = deal(ranges_of_sizes(ds.granules, std::iter::repeat(2)), 12, 0xC0FFEE);
        assert_single_pass_multiset(&ds, &sched, ds.granules * ds.rows_per_granule, true);
    }

    #[test]
    fn single_pass_all_identical_hash_no_lost_updates() {
        // The pathological limit: every row in ONE bucket, one chain head —
        // the CAS is fully serialized by contention. All tuples must survive.
        let ds = Dataset {
            granules: 48,
            rows_per_granule: 50,
            key_space: 1, // single hash ⇒ single bucket
            seed: 0x515,
            force_partition: Some(0x11),
        };
        let sched = deal(ranges_of_sizes(ds.granules, std::iter::repeat(1)), 8, 0xA5A5);
        assert_single_pass_multiset(&ds, &sched, 2048, true);
    }

    #[test]
    fn single_pass_underestimate_triggers_grow_buckets() {
        // Estimate 64 rows; true count is 64*37 = 2368 ⇒ load factor forces a
        // barrier-gated grow at seal. The grown table must still hold the
        // exact multiset (grow preserves every tuple).
        let ds = ds_default();
        let budget = JoinBudget::unlimited();
        let sched = deal(ranges_of_sizes(ds.granules, std::iter::repeat(3)), 6, 0x9);
        let t = build_single_pass(&ds, &sched, 64, &budget, true);
        assert!(t.nbuckets() as u64 >= (ds.granules * ds.rows_per_granule) / GROW_LOAD_FACTOR, "grow must have enlarged the table");
        assert_eq!(t.total_tuples(), ds.granules * ds.rows_per_granule);
        assert_eq!(frozen_multiset(&t), sorted_rows(&ds), "grow_buckets dropped/duplicated tuples");
    }

    #[test]
    fn single_pass_overestimate_keeps_estimate_size_no_grow() {
        let ds = ds_default();
        let budget = JoinBudget::unlimited();
        let sched = vec![vec![0..ds.granules]];
        // Estimate 10x the truth: no grow, table sized from the estimate.
        let est = 10 * ds.granules * ds.rows_per_granule;
        let t = build_single_pass(&ds, &sched, est, &budget, false);
        assert_eq!(t.nbuckets() as u64, est.next_power_of_two());
        assert_eq!(frozen_multiset(&t), sorted_rows(&ds));
    }

    #[test]
    fn single_pass_empty_build() {
        let budget = JoinBudget::unlimited();
        let dir = SharedBuildDir::with_estimate(1000, &budget).unwrap();
        let plan = Arc::new(finish_single_pass(&[], dir, &budget).unwrap());
        let t = freeze(plan, &[]);
        assert_eq!(t.total_tuples(), 0);
        assert_eq!(t.chain(0xDEAD_BEEF).count(), 0);
        assert!(!t.has_seat(), "single-pass never builds the dense seat");
    }

    #[test]
    fn single_pass_never_seats() {
        // Plain (un-keyed) single-pass pushes forgo the seat — the SE-MBSEAT
        // order-free seat exists ONLY when the runtime armed key tracking
        // (see single_pass_armed_seat_*). The v1 probe still answers.
        let ds = ds_default();
        let sched = deal(ranges_of_sizes(ds.granules, std::iter::repeat(4)), 5, 0x33);
        let budget = JoinBudget::unlimited();
        let t = build_single_pass(&ds, &sched, ds.granules * ds.rows_per_granule, &budget, true);
        assert!(!t.has_seat());
    }

    /// SE-MBSEAT: keyed single-pass build helper — key = a small int derived
    /// from the row (NULL_KEY every `null_every`-th row when nonzero).
    fn build_single_pass_keyed(
        nworkers: usize,
        rows_per_worker: u64,
        key_space: i64,
        key_stride: i64,
        key_base: i64,
        null_every: u64,
        budget: &Arc<JoinBudget>,
    ) -> (FrozenJoinTable, Vec<(i64, Vec<u8>)>) {
        let dir = SharedBuildDir::with_estimate(nworkers as u64 * rows_per_worker, budget)
            .expect("dir within budget");
        let mut locals: Vec<JoinBuildLocal> = (0..nworkers)
            .map(|w| {
                let mut l = JoinBuildLocal::new(w, Arc::clone(budget));
                l.attach_shared_dir(Arc::clone(&dir));
                l.arm_singlepass_keys();
                l
            })
            .collect();
        let mut expect: Vec<(i64, Vec<u8>)> = Vec::new();
        for (w, l) in locals.iter_mut().enumerate() {
            l.begin_run(w as u64 * rows_per_worker);
            for i in 0..rows_per_worker {
                let n = w as u64 * rows_per_worker + i;
                let payload = n.to_le_bytes().to_vec();
                let h = mix(n) as u32;
                let key = if null_every != 0 && n % null_every == 0 {
                    NULL_KEY
                } else {
                    key_base + (n as i64 % key_space) * key_stride
                };
                l.push_keyed(h, &payload, key).unwrap();
                if key != NULL_KEY {
                    expect.push((key, payload));
                }
            }
            l.end_run();
        }
        let plan = Arc::new(finish_single_pass(&locals, dir, budget).expect("finish"));
        let t = freeze(Arc::clone(&plan), &locals);
        drop(locals);
        (t, expect)
    }

    #[test]
    fn single_pass_armed_seat_candidates_match_reference() {
        // Order-free contract: per-key candidate PAYLOAD MULTISETS equal the
        // reference (order deliberately unasserted — the multibuild consumer
        // is order-insensitive); NULL keys sit in no slice; out-of-range
        // probes answer empty.
        let budget = JoinBudget::unlimited();
        let (t, mut expect) =
            build_single_pass_keyed(4, 200, 37, 1, -5, 7, &budget);
        let seat = t.seat().expect("armed dense-int build must seat");
        let mut got: Vec<(i64, Vec<u8>)> = Vec::new();
        for k in -5..(-5 + 37) {
            for &r in seat.candidates(k as i32) {
                got.push((k, t.tuple_ref(r).payload().to_vec()));
            }
        }
        got.sort();
        expect.sort();
        assert_eq!(got, expect, "seat candidate multiset diverges from the reference");
        assert!(seat.candidates(i32::MIN + 1).is_empty());
        assert!(seat.candidates(1 << 20).is_empty());
    }

    #[test]
    fn single_pass_armed_seat_respects_range_gate() {
        // Sparse keys (range >> 4x rows): the seat is forgone, the build
        // stands, the v1 probe answers — never a refusal.
        let budget = JoinBudget::unlimited();
        let (t, expect) =
            build_single_pass_keyed(2, 50, 100, 1 << 14, 0, 0, &budget);
        assert!(!t.has_seat(), "sparse keys must forgo the seat");
        assert_eq!(t.total_tuples() as usize, expect.len());
    }

    #[test]
    fn single_pass_armed_seat_all_null_keys_forgo() {
        let budget = JoinBudget::unlimited();
        let (t, expect) = build_single_pass_keyed(2, 40, 5, 1, 0, 1, &budget);
        assert!(expect.is_empty());
        assert!(!t.has_seat(), "an all-NULL key stream seats nothing");
        assert_eq!(t.total_tuples(), 80);
    }

    #[test]
    fn boundary_guard_estimator_covers_this_representation() {
        // GL-HJMB-1 escalation A: nodehash::estimate_runtime_hj_build_peak_bytes
        // prices THIS module's representation (HDR_WORDS + the flat 8B ref
        // charge + whole-word images + the seal bucket arithmetic). If these
        // constants drift under it, the admission/probe boundary guards
        // under-predict and the demote-unsafe seal refusal (an R5 serial
        // rerun measured 5-11x worse than legacy Parallel Hash) comes back.
        // Bound both sides: estimate >= the ACTUAL charged accounting (with
        // min-size chunks so allocator tail slack stays marginal), and
        // within 2x of it (a uselessly conservative estimator would eat the
        // engaged-spill band).
        for &(ntuples, width) in &[(50_000u64, 8usize), (20_000, 40), (5_000, 128)] {
            let budget = JoinBudget::new(usize::MAX);
            // Min chunk cap: capacity tail <= 64KB, so the comparison
            // exercises the per-tuple constants, not chunk-growth slack
            // (which the estimator's 1/8 headroom owns at scale).
            let mut l = JoinBuildLocal::with_chunk_cap(0, Arc::clone(&budget), CHUNK_MIN_WORDS);
            // The estimator's assumed image: maxaligned minimal header (15
            // -> 16) + maxaligned data width.
            let image = 16 + width.div_ceil(8) * 8;
            let payload = vec![0u8; image];
            l.begin_run(0);
            for i in 0..ntuples {
                l.push(mix(i) as u32, &payload).expect("unlimited budget");
            }
            l.end_run();
            let buckets = 8 * ntuples.max(1).next_power_of_two().clamp(1024, 1 << 31);
            let actual = budget.used() as u64 + buckets;
            let est =
                ::nodehash::estimate_runtime_hj_build_peak_bytes(ntuples as f64, width as i32);
            assert!(
                est >= actual,
                "boundary-guard estimator UNDER-predicts the arm representation: \
                 est={est} actual={actual} (n={ntuples} w={width}) — the demote-unsafe \
                 seal-refusal band reopens"
            );
            assert!(
                est <= actual.saturating_mul(2),
                "boundary-guard estimator uselessly conservative: est={est} actual={actual} \
                 (n={ntuples} w={width})"
            );
        }
    }

    #[test]
    fn single_pass_dir_budget_crossing_is_recoverable() {
        // A directory that won't fit returns BudgetExceeded with the failed
        // charge BACKED OUT — the runtime falls back to two-pass against the
        // SAME budget (never a refusal on account of single-pass alone), so
        // the fallback build must GENUINELY PROCEED within what remains.
        let ds = Dataset {
            granules: 8,
            rows_per_granule: 20,
            key_space: 31,
            seed: 0xB0D6E,
            force_partition: None,
        };
        // Two-pass needs ~one 64KB chunk + 160 ref-words + the 1024-bucket
        // plan array (~73KB); the 1M-row directory (8MB) never fits.
        let budget = JoinBudget::new(CHUNK_MIN_WORDS * 8 + 64 * 1024);
        assert_eq!(SharedBuildDir::with_estimate(1_000_000, &budget).err(), Some(BudgetExceeded));
        assert_eq!(
            budget.used(),
            0,
            "the failed dir charge must be backed out, not poison the envelope"
        );
        // The documented fallback, on the SAME budget: the two-pass build
        // must complete and match the serial oracle byte-for-byte.
        let locals = build_from_schedule(&ds, &vec![vec![0..ds.granules]], &budget)
            .expect("two-pass fallback must fit once the dir charge is released");
        let t = plan_combine_freeze(locals, &budget, false);
        assert_eq!(t.total_tuples(), ds.granules * ds.rows_per_granule);
        let l2 = (t.nbuckets() as u64).trailing_zeros();
        assert_eq!(frozen_chains(&t), reference_chains(&ds.all_rows(), l2));
    }

    #[test]
    fn grow_buckets_releases_the_superseded_dir_charge() {
        // Underestimate forces the seal-time grow; the estimate-sized
        // array's charge must be RELEASED when the grown array is charged,
        // so the envelope ends holding chunks + refs + ONE directory.
        let ds = ds_default();
        let budget = JoinBudget::unlimited();
        let dir = SharedBuildDir::with_estimate(64, &budget).expect("unlimited");
        let old_dir_bytes = dir.nbuckets() * 8;
        let mut l = JoinBuildLocal::new(0, Arc::clone(&budget));
        l.attach_shared_dir(Arc::clone(&dir));
        l.begin_run(0);
        for g in 0..ds.granules {
            for (h, p) in ds.rows_of(g) {
                l.push(h, &p).unwrap();
            }
        }
        l.end_run();
        let locals = vec![l];
        let used_before_seal = budget.used();
        let plan = finish_single_pass(&locals, dir, &budget).expect("unlimited");
        assert!(plan.singledir.is_none(), "underestimate must grow into a plan-owned array");
        let new_dir_bytes = plan.buckets.len() * 8;
        assert!(new_dir_bytes > old_dir_bytes);
        assert_eq!(
            budget.used(),
            used_before_seal - old_dir_bytes + new_dir_bytes,
            "the superseded estimate-sized array must be un-charged at grow"
        );
        // The grown table still answers (belt and braces).
        let t = freeze(Arc::new(plan), &locals);
        drop(locals);
        assert_eq!(frozen_multiset(&t), sorted_rows(&ds));
    }

    // ---- LOOM: concurrent CAS head-insert (the singlepass insert core) ----
    //
    // Models `cas_insert_head`'s Treiber push against loom atomics: N workers
    // race to link their tuples at one bucket head. The properties under EVERY
    // interleaving loom explores: NO lost updates (every inserted ref appears
    // exactly once in the final chain), NO cycle, and the 16-bit TAG WORD is
    // exactly the OR of every inserted tuple's tag — the `(old & 0xFFFF) | tag`
    // maintenance rides the same CAS, and a regression that clobbers it would
    // re-open the probe pre-filter's false-negative hole (a dropped tag bit
    // hides a whole chain from `chain()`). This is the correctness contract
    // the production `cas_insert_head` (std atomics) must uphold; the loop
    // below mirrors it verbatim, tag included. loom drives its OWN atomics, so
    // it runs under plain `cargo test -p nodehashjoin` (no --cfg loom needed).
    mod singlepass_loom {
        use loom::sync::atomic::{AtomicU64, Ordering};
        use loom::sync::Arc;

        /// Mirror of super::cas_insert_head over loom atomics, INCLUDING the
        /// tag-word maintenance (`(old & 0xFFFF) | tag` under the same CAS).
        fn cas_insert_head(bucket: &AtomicU64, next_word: &AtomicU64, packed_ref: u64, tag: u64) {
            let mut old = bucket.load(Ordering::Relaxed);
            loop {
                next_word.store(old >> 16, Ordering::Relaxed);
                let newv = ((packed_ref + 1) << 16) | ((old & 0xFFFF) | tag);
                match bucket.compare_exchange_weak(old, newv, Ordering::Release, Ordering::Relaxed) {
                    Ok(_) => return,
                    Err(cur) => old = cur,
                }
            }
        }

        /// Each modeled ref carries its own distinct tag bit, so a clobbered
        /// (dropped or stray) bit is unambiguously attributable.
        fn tag_of(r: u64) -> u64 {
            1 << r
        }

        /// Walk the final chain and assert every ref in `0..n` appears once,
        /// and that the bucket's tag word is EXACTLY the OR of all inserted
        /// tags — no dropped bits (a lost `old & 0xFFFF`), no stray bits.
        fn assert_all_present(bucket: &AtomicU64, nexts: &[AtomicU64], n: u64) {
            let word = bucket.load(Ordering::Acquire);
            let expect_tags = (0..n).fold(0u64, |acc, r| acc | tag_of(r));
            assert_eq!(
                word & 0xFFFF,
                expect_tags,
                "tag word diverges from the OR of inserted tags"
            );
            let mut seen = vec![false; n as usize];
            let mut cur = word >> 16;
            let mut steps = 0u64;
            while cur != 0 {
                let r = cur - 1;
                assert!(!seen[r as usize], "cycle/duplicate ref {r}");
                seen[r as usize] = true;
                steps += 1;
                assert!(steps <= n, "chain longer than inserted");
                // The per-tuple next word holds a bare ref+1 (cas_insert_head
                // stores `old >> 16`), NOT a shifted bucket word — no >> 16.
                cur = nexts[r as usize].load(Ordering::Acquire);
            }
            assert!(seen.iter().all(|&s| s), "lost a tuple under concurrent CAS");
        }

        #[test]
        fn two_workers_one_bucket_no_lost_updates() {
            // Worker A inserts refs {0,1}; worker B inserts ref {2}. One
            // bucket ⇒ full head contention. 3 tuples keeps loom tractable.
            // Every ref carries a distinct tag bit; the final word must OR
            // all three (tag-clobber coverage).
            loom::model(|| {
                const N: u64 = 3;
                let bucket = Arc::new(AtomicU64::new(0));
                let nexts: Arc<Vec<AtomicU64>> =
                    Arc::new((0..N).map(|_| AtomicU64::new(0)).collect());

                let a = {
                    let (bucket, nexts) = (bucket.clone(), nexts.clone());
                    loom::thread::spawn(move || {
                        cas_insert_head(&bucket, &nexts[0], 0, tag_of(0));
                        cas_insert_head(&bucket, &nexts[1], 1, tag_of(1));
                    })
                };
                let b = {
                    let (bucket, nexts) = (bucket.clone(), nexts.clone());
                    loom::thread::spawn(move || {
                        cas_insert_head(&bucket, &nexts[2], 2, tag_of(2));
                    })
                };
                a.join().unwrap();
                b.join().unwrap();
                assert_all_present(&bucket, &nexts, N);
            });
        }
    }
}
