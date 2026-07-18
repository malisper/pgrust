// nodeHash.c parallel arms, thread-native: dsa_pointer = raw pointer, DSA
// chunks = global-heap allocations owned by the shared batch state, DSM entry
// = Arc, pstate LWLock = Mutex. Lock order: pstate before any batch mutex.
// Space accounting keeps C's constants (EXPLAIN byte-parity).
#![allow(non_snake_case)]

use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::fd::fileset::FileSet;
use ::mcx::Mcx;
use ::pg_barrier::Barrier;
use ::sharedtuplestore::{SharedTuplestore, SharedTuplestoreAccessor};
use ::types_core::instrument::HashInstrumentation;
use ::types_error::PgResult;
use ::types_tuple::MinimalTupleData;

use crate::{
    exec_choose_hash_table_size_full, get_hash_memory_limit, HashJoinTupleHdr, HashState,
    HJTUPLE_OVERHEAD,
};

const HASH_CHUNK_SIZE: usize = 32 * 1024;
// MAXALIGN(sizeof(HashMemoryChunkData)) on a 64-bit build.
const HASH_CHUNK_HEADER_SIZE: usize = 32;
const HASH_CHUNK_THRESHOLD: usize = HASH_CHUNK_SIZE / 4;
const NTUP_PER_BUCKET: usize = 1;
const MAX_ALLOC_SIZE: usize = 0x3fff_ffff;
const SIZEOF_BUCKET: usize = 8; // sizeof(dsa_pointer_atomic)

// Build-barrier phases.
pub const PHJ_BUILD_ELECT: i32 = 0;
pub const PHJ_BUILD_ALLOCATE: i32 = 1;
pub const PHJ_BUILD_HASH_INNER: i32 = 2;
pub const PHJ_BUILD_HASH_OUTER: i32 = 3;
pub const PHJ_BUILD_RUN: i32 = 4;
pub const PHJ_BUILD_FREE: i32 = 5;

// Batch-barrier phases.
pub const PHJ_BATCH_ELECT: i32 = 0;
pub const PHJ_BATCH_ALLOCATE: i32 = 1;
pub const PHJ_BATCH_LOAD: i32 = 2;
pub const PHJ_BATCH_PROBE: i32 = 3;
pub const PHJ_BATCH_SCAN: i32 = 4;
pub const PHJ_BATCH_FREE: i32 = 5;

const PHJ_GROW_BATCHES_ELECT: i32 = 0;
const PHJ_GROW_BATCHES_PHASES: i32 = 5;
const PHJ_GROW_BUCKETS_ELECT: i32 = 0;
const PHJ_GROW_BUCKETS_PHASES: i32 = 3;

fn grow_batches_phase(n: i32) -> i32 {
    n % PHJ_GROW_BATCHES_PHASES
}

fn grow_buckets_phase(n: i32) -> i32 {
    n % PHJ_GROW_BUCKETS_PHASES
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ParallelHashGrowth {
    Ok,
    NeedMoreBuckets,
    NeedMoreBatches,
    Disabled,
}

/// C `HashMemoryChunkData`; tuples follow at `HASH_CHUNK_HEADER_SIZE`,
/// MAXALIGN-packed, so the repartition walks can replay them by offset.
#[repr(C)]
pub struct HashMemoryChunkHdr {
    ntuples: i32,
    _pad: u32,
    maxlen: usize,
    used: usize,
    next: *mut HashMemoryChunkHdr,
}

// wasm32: 4-byte usize/pointer fields pack the header under the 64-bit
// constant; all offset arithmetic uses HASH_CHUNK_HEADER_SIZE (not
// size_of), so layout stays self-consistent — the pin documents the
// native cost shape only.
#[cfg(not(target_family = "wasm"))]
const _: () = assert!(core::mem::size_of::<HashMemoryChunkHdr>() == HASH_CHUNK_HEADER_SIZE);

fn chunk_layout(maxlen: usize) -> core::alloc::Layout {
    core::alloc::Layout::from_size_align(HASH_CHUNK_HEADER_SIZE + maxlen, 8)
        .expect("chunk layout fits")
}

// dsa_allocate analog; freed by free_chunk at C's dsa_free points.
fn alloc_chunk(chunk_size: usize) -> *mut HashMemoryChunkHdr {
    let maxlen = chunk_size - HASH_CHUNK_HEADER_SIZE;
    // SAFETY: layout is non-zero; header initialized immediately below.
    unsafe {
        let p = std::alloc::alloc(chunk_layout(maxlen)).cast::<HashMemoryChunkHdr>();
        assert!(!p.is_null(), "out of memory allocating hash chunk");
        (*p).ntuples = 0;
        (*p)._pad = 0;
        (*p).maxlen = maxlen;
        (*p).used = 0;
        (*p).next = core::ptr::null_mut();
        p
    }
}

// SAFETY: `chunk` came from alloc_chunk and is not referenced afterwards.
unsafe fn free_chunk(chunk: *mut HashMemoryChunkHdr) {
    // SAFETY: caller contract.
    unsafe {
        let maxlen = (*chunk).maxlen;
        std::alloc::dealloc(chunk.cast(), chunk_layout(maxlen));
    }
}

// SAFETY: `chunk` is live; offsets in [0, used) were written by tuple_at's
// writers as MAXALIGN-packed HashJoinTuple images.
unsafe fn tuple_at(chunk: *mut HashMemoryChunkHdr, idx: usize) -> *mut HashJoinTupleHdr {
    // SAFETY: caller contract.
    unsafe { chunk.cast::<u8>().add(HASH_CHUNK_HEADER_SIZE + idx).cast() }
}

struct SendPtr<T>(*mut T);
// SAFETY: shared-arena pointers whose cross-thread discipline is the PHJ
// barrier/lock protocol ported from C.
unsafe impl<T> Send for SendPtr<T> {}
// SAFETY: as above.
unsafe impl<T> Sync for SendPtr<T> {}

/// Mutable shared per-batch state; C guards these fields with pstate->lock
/// or barrier-exclusive phases. Lock order: pstate lock, then this.
struct BatchShared {
    buckets: Option<Box<[AtomicPtr<HashJoinTupleHdr>]>>,
    chunks: SendPtr<HashMemoryChunkHdr>,
    size: usize,
    estimated_size: usize,
    ntuples: usize,
    old_ntuples: usize,
    space_exhausted: bool,
}

/// C `ParallelHashJoinBatch` (+ its two SharedTuplestores).
pub struct ParallelHashJoinBatch {
    batch_barrier: Barrier,
    mu: Mutex<BatchShared>,
    // Written racily during PHJ_BATCH_PROBE, read in PHJ_BATCH_SCAN (C keeps
    // it a plain bool with the same argument).
    skip_unmatched: AtomicBool,
    inner_tuples: Arc<SharedTuplestore>,
    outer_tuples: Arc<SharedTuplestore>,
}

impl ParallelHashJoinBatch {
    fn lock(&self) -> MutexGuard<'_, BatchShared> {
        self.mu.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Fields C guards with `pstate->lock`.
pub struct PhjLocked {
    batches: Option<Arc<[ParallelHashJoinBatch]>>,
    old_batches: Option<Arc<[ParallelHashJoinBatch]>>,
    pub nbatch: i32,
    old_nbatch: i32,
    pub nbuckets: u32,
    growth: ParallelHashGrowth,
    chunk_work_queue: SendPtr<HashMemoryChunkHdr>,
    pub space_allowed: usize,
    pub total_tuples: usize,
}

/// C `ParallelHashJoinState` (the DSM shm_toc entry).
pub struct ParallelHashJoinState {
    pub lock: Mutex<PhjLocked>,
    pub build_barrier: Barrier,
    grow_batches_barrier: Barrier,
    grow_buckets_barrier: Barrier,
    distributor: AtomicU32,
    pub nparticipants: i32,
    pub fileset: Arc<FileSet>,
}

impl ParallelHashJoinState {
    /// `ExecHashJoinInitializeDSM` shared-state part.
    pub fn new(nparticipants: i32) -> PgResult<ParallelHashJoinState> {
        Ok(ParallelHashJoinState {
            lock: Mutex::new(PhjLocked {
                batches: None,
                old_batches: None,
                nbatch: 0,
                old_nbatch: 0,
                nbuckets: 0,
                growth: ParallelHashGrowth::Ok,
                chunk_work_queue: SendPtr(core::ptr::null_mut()),
                space_allowed: 0,
                total_tuples: 0,
            }),
            build_barrier: Barrier::new(0),
            grow_batches_barrier: Barrier::new(0),
            grow_buckets_barrier: Barrier::new(0),
            distributor: AtomicU32::new(0),
            nparticipants,
            fileset: Arc::new(FileSet::init()?),
        })
    }

    /// `ExecHashJoinReInitializeDSM`: fresh barriers/state for a rescan; the
    /// caller has already detached everything.
    pub fn reinitialize(&self) -> PgResult<()> {
        {
            let mut g = self.locked();
            debug_assert!(g.batches.is_none());
            g.old_batches = None;
            g.nbatch = 0;
            g.old_nbatch = 0;
            g.nbuckets = 0;
            g.growth = ParallelHashGrowth::Ok;
            g.chunk_work_queue = SendPtr(core::ptr::null_mut());
            g.total_tuples = 0;
        }
        self.fileset.delete_all()?;
        // C re-runs BarrierInit on build_barrier so the next scan starts at
        // PHJ_BUILD_ELECT.
        self.build_barrier.reset();
        self.grow_batches_barrier.reset();
        self.grow_buckets_barrier.reset();
        self.distributor.store(0, Ordering::Relaxed);
        Ok(())
    }

    pub fn locked(&self) -> MutexGuard<'_, PhjLocked> {
        self.lock.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Per-participant per-batch accessor state (C
/// `ParallelHashJoinBatchAccessor`); `shared` is (generation, index).
struct BatchAccessor<'mcx> {
    preallocated: usize,
    ntuples: usize,
    size: usize,
    estimated_size: usize,
    old_ntuples: usize,
    at_least_one_chunk: bool,
    outer_eof: bool,
    done: bool,
    inner_tuples: SharedTuplestoreAccessor<'mcx>,
    outer_tuples: SharedTuplestoreAccessor<'mcx>,
}

/// The parallel arm of C `HashJoinTableData`, kept separate so the serial
/// table stays untouched.
pub struct ParallelHashJoinTable<'mcx> {
    pub pstate: Arc<ParallelHashJoinState>,
    mcx: Mcx<'mcx>,
    participant: i32,

    pub nbuckets: u32,
    log2_nbuckets: u32,
    nbuckets_original: u32,
    // Cached &[AtomicPtr] of the current batch (C hashtable->buckets.shared).
    buckets_ptr: *const AtomicPtr<HashJoinTupleHdr>,

    pub nbatch: i32,
    pub curbatch: i32,
    nbatch_original: i32,

    pub total_tuples: f64,
    pub partial_tuples: f64,

    space_peak: usize,

    batches_gen: Option<Arc<[ParallelHashJoinBatch]>>,
    batches: Vec<BatchAccessor<'mcx>>,

    current_chunk: *mut HashMemoryChunkHdr,
    // C's hashtable->parallel_state = NULL after ExecHashTableDetach.
    detached: bool,
}

impl<'mcx> ParallelHashJoinTable<'mcx> {
    fn shared_batch(&self, i: i32) -> &ParallelHashJoinBatch {
        &self.batches_gen.as_ref().expect("batches installed")[i as usize]
    }

    pub fn batch_barrier(&self, i: i32) -> &Barrier {
        &self.shared_batch(i).batch_barrier
    }

    pub fn skip_unmatched(&self, i: i32) -> bool {
        self.shared_batch(i).skip_unmatched.load(Ordering::Relaxed)
    }

    pub fn set_skip_unmatched(&self, i: i32) {
        self.shared_batch(i).skip_unmatched.store(true, Ordering::Relaxed);
    }

    pub fn batch_done(&self, i: i32) -> bool {
        self.batches[i as usize].done
    }

    pub fn set_batch_done(&mut self, i: i32) {
        self.batches[i as usize].done = true;
    }

    pub fn set_outer_eof(&mut self, i: i32) {
        self.batches[i as usize].outer_eof = true;
    }

    pub fn outer_eof(&self, i: i32) -> bool {
        self.batches[i as usize].outer_eof
    }

    pub fn distribute_batchno(&self) -> i32 {
        (self.pstate.distributor.fetch_add(1, Ordering::Relaxed) % self.nbatch as u32) as i32
    }

    pub fn inner_tuples(&mut self, i: i32) -> &mut SharedTuplestoreAccessor<'mcx> {
        &mut self.batches[i as usize].inner_tuples
    }

    pub fn outer_tuples(&mut self, i: i32) -> &mut SharedTuplestoreAccessor<'mcx> {
        &mut self.batches[i as usize].outer_tuples
    }

    /// `ExecHashGetBucketAndBatch` (parallel table copy; same arithmetic).
    #[inline]
    pub fn get_bucket_and_batch(&self, hashvalue: u32) -> (u32, i32) {
        let nbuckets = self.nbuckets;
        let nbatch = self.nbatch as u32;
        if nbatch > 1 {
            (
                hashvalue & (nbuckets - 1),
                (hashvalue.rotate_right(self.log2_nbuckets) & (nbatch - 1)) as i32,
            )
        } else {
            (hashvalue & (nbuckets - 1), 0)
        }
    }

    /// `ExecParallelHashFirstTuple`.
    #[inline]
    pub fn first_tuple(&self, bucketno: u32) -> *mut HashJoinTupleHdr {
        debug_assert!(bucketno < self.nbuckets && !self.buckets_ptr.is_null());
        // SAFETY: buckets_ptr caches the current batch's live bucket array
        // (ExecParallelHashTableSetCurrentBatch), masked index.
        unsafe { (*self.buckets_ptr.add(bucketno as usize)).load(Ordering::Acquire) }
    }

    /// `ExecParallelHashNextTuple`.
    #[inline]
    pub fn next_tuple(&self, tuple: *mut HashJoinTupleHdr) -> *mut HashJoinTupleHdr {
        // SAFETY: live chain entry in the shared arena.
        unsafe { (*tuple).next() }
    }

    // ExecParallelHashPushTuple: CAS-prepend to the bucket chain.
    #[inline]
    fn push_tuple(&self, bucketno: u32, tuple: *mut HashJoinTupleHdr) {
        // SAFETY: as first_tuple; tuple is a fresh allocation this thread owns.
        unsafe {
            let head = &*self.buckets_ptr.add(bucketno as usize);
            let mut cur = head.load(Ordering::Relaxed);
            loop {
                (*tuple).set_next(cur);
                match head.compare_exchange_weak(
                    cur,
                    tuple,
                    Ordering::Release,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(actual) => cur = actual,
                }
            }
        }
    }

    pub fn instrumentation(&self) -> HashInstrumentation {
        HashInstrumentation {
            nbuckets: self.nbuckets as i32,
            nbuckets_original: self.nbuckets_original as i32,
            nbatch: self.nbatch,
            nbatch_original: self.nbatch_original,
            space_peak: self.space_peak as u64,
        }
    }
}

fn make_batch(
    pstate: &ParallelHashJoinState,
    batchno: i32,
    nbatch: i32,
) -> ParallelHashJoinBatch {
    let batch = ParallelHashJoinBatch {
        batch_barrier: Barrier::new(0),
        mu: Mutex::new(BatchShared {
            buckets: None,
            chunks: SendPtr(core::ptr::null_mut()),
            size: 0,
            estimated_size: 0,
            ntuples: 0,
            old_ntuples: 0,
            space_exhausted: false,
        }),
        skip_unmatched: AtomicBool::new(false),
        inner_tuples: Arc::new(SharedTuplestore::new(
            pstate.nparticipants,
            core::mem::size_of::<u32>(),
            &format!("i{batchno}of{nbatch}"),
        )),
        outer_tuples: Arc::new(SharedTuplestore::new(
            pstate.nparticipants,
            core::mem::size_of::<u32>(),
            &format!("o{batchno}of{nbatch}"),
        )),
    };
    if batchno == 0 {
        // Batch 0 loads while hashing: pre-advance to PHJ_BATCH_PROBE.
        batch.batch_barrier.attach();
        while batch.batch_barrier.phase() < PHJ_BATCH_PROBE {
            batch
                .batch_barrier
                .arrive_and_wait()
                .expect("sole participant cannot block");
        }
        batch.batch_barrier.detach();
    }
    batch
}

// ExecParallelHashJoinSetUpBatches: one backend creates the generation.
fn setup_batches(table: &mut ParallelHashJoinTable<'_>, nbatch: i32) {
    debug_assert!(table.batches.is_empty());
    let pstate = Arc::clone(&table.pstate);
    let gen: Arc<[ParallelHashJoinBatch]> =
        (0..nbatch).map(|i| make_batch(&pstate, i, nbatch)).collect();
    {
        let mut g = pstate.locked();
        g.batches = Some(Arc::clone(&gen));
        g.nbatch = nbatch;
    }
    table.nbatch = nbatch;
    table.batches_gen = Some(Arc::clone(&gen));
    table.batches = (0..nbatch)
        .map(|i| make_accessor(table.mcx, &gen[i as usize], table.participant, &pstate))
        .collect();
}

fn make_accessor<'mcx>(
    mcx: Mcx<'mcx>,
    shared: &ParallelHashJoinBatch,
    participant: i32,
    pstate: &ParallelHashJoinState,
) -> BatchAccessor<'mcx> {
    BatchAccessor {
        preallocated: 0,
        ntuples: 0,
        size: 0,
        estimated_size: 0,
        old_ntuples: 0,
        at_least_one_chunk: false,
        outer_eof: false,
        done: false,
        inner_tuples: SharedTuplestoreAccessor::attach(
            Arc::clone(&shared.inner_tuples),
            Arc::clone(&pstate.fileset),
            participant,
            mcx,
        ),
        outer_tuples: SharedTuplestoreAccessor::attach(
            Arc::clone(&shared.outer_tuples),
            Arc::clone(&pstate.fileset),
            participant,
            mcx,
        ),
    }
}

// ExecParallelHashCloseBatchAccessors.
fn close_batch_accessors(table: &mut ParallelHashJoinTable<'_>) -> PgResult<()> {
    for a in table.batches.iter_mut() {
        a.inner_tuples.end_write()?;
        a.outer_tuples.end_write()?;
        a.inner_tuples.end_parallel_scan()?;
        a.outer_tuples.end_parallel_scan()?;
    }
    table.batches.clear();
    table.batches_gen = None;
    Ok(())
}

// ExecParallelHashEnsureBatchAccessors.
fn ensure_batch_accessors(table: &mut ParallelHashJoinTable<'_>) -> PgResult<()> {
    let pstate = Arc::clone(&table.pstate);
    let (gen, nbatch) = {
        let g = pstate.locked();
        (
            Arc::clone(g.batches.as_ref().expect("batch state not freed")),
            g.nbatch,
        )
    };
    if !table.batches.is_empty() {
        if table.nbatch == nbatch
            && table
                .batches_gen
                .as_ref()
                .is_some_and(|old| Arc::ptr_eq(old, &gen))
        {
            return Ok(());
        }
        close_batch_accessors(table)?;
    }
    table.nbatch = nbatch;
    table.batches_gen = Some(Arc::clone(&gen));
    table.batches = (0..nbatch)
        .map(|i| make_accessor(table.mcx, &gen[i as usize], table.participant, &pstate))
        .collect();
    Ok(())
}

fn alloc_buckets(nbuckets: u32) -> Box<[AtomicPtr<HashJoinTupleHdr>]> {
    (0..nbuckets).map(|_| AtomicPtr::new(core::ptr::null_mut())).collect()
}

/// `ExecParallelHashTableAlloc`.
pub fn exec_parallel_hash_table_alloc(table: &ParallelHashJoinTable<'_>, batchno: i32) {
    let nbuckets = table.pstate.locked().nbuckets;
    let batch = table.shared_batch(batchno);
    batch.lock().buckets = Some(alloc_buckets(nbuckets));
}

/// `ExecParallelHashTableSetCurrentBatch`.
pub fn exec_parallel_hash_table_set_current_batch(
    table: &mut ParallelHashJoinTable<'_>,
    batchno: i32,
) {
    table.curbatch = batchno;
    let nbuckets = table.pstate.locked().nbuckets;
    let buckets_ptr = {
        let b = table.shared_batch(batchno).lock();
        let buckets = b.buckets.as_ref().expect("batch buckets allocated");
        debug_assert!(buckets.len() == nbuckets as usize);
        buckets.as_ptr()
    };
    table.buckets_ptr = buckets_ptr;
    table.nbuckets = nbuckets;
    table.log2_nbuckets = nbuckets.trailing_zeros();
    table.current_chunk = core::ptr::null_mut();
    table.batches[batchno as usize].at_least_one_chunk = false;
}

/// `ExecHashTableCreate`, parallel arm; the serial arm stays in lib.rs.
pub fn exec_parallel_hash_table_create<'mcx>(
    hs: &HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<ParallelHashJoinTable<'mcx>> {
    let pstate = Arc::clone(hs.parallel_state.as_ref().expect("parallel hash state attached"));
    let mcx = estate.es_query_cxt;
    // useskew mirrors C's OidIsValid(node->skewTable) exactly as the serial
    // create (sizing reserves skew memory even though parallel hash never
    // builds the skew table).
    let (nbuckets, nbatch, _num_skew_mcvs, space_allowed) = exec_choose_hash_table_size_full(
        hs.ntuples_est,
        hs.tupwidth,
        true,
        true,
        pstate.nparticipants - 1,
    );

    let mut table = ParallelHashJoinTable {
        pstate: Arc::clone(&pstate),
        mcx,
        participant: participant_number(),
        nbuckets,
        log2_nbuckets: nbuckets.trailing_zeros(),
        nbuckets_original: nbuckets,
        buckets_ptr: core::ptr::null(),
        nbatch,
        curbatch: 0,
        nbatch_original: nbatch,
        total_tuples: 0.0,
        partial_tuples: 0.0,
        space_peak: 0,
        batches_gen: None,
        batches: Vec::new(),
        current_chunk: core::ptr::null_mut(),
        detached: false,
    };

    // Attach; detach is ExecHashTableDetach.
    let build_barrier = &pstate.build_barrier;
    build_barrier.attach();
    if build_barrier.phase() == PHJ_BUILD_ELECT && build_barrier.arrive_and_wait()? {
        {
            let mut g = pstate.locked();
            g.space_allowed = space_allowed;
            g.growth = ParallelHashGrowth::Ok;
            g.nbuckets = nbuckets;
        }
        setup_batches(&mut table, nbatch);
        exec_parallel_hash_table_alloc(&table, 0);
    }
    Ok(table)
}

fn participant_number() -> i32 {
    // ParallelWorkerNumber + 1; the leader (-1) is participant 0.
    ::parallel::ParallelWorkerNumber() + 1
}

/// `MultiExecParallelHash`'s build loop body is driven by the caller (the
/// dispatcher owns the child); this covers the barrier choreography before
/// and after the insert loop.
pub fn multi_exec_parallel_hash_begin<'mcx>(
    table: &mut ParallelHashJoinTable<'mcx>,
) -> PgResult<bool> {
    let pstate = Arc::clone(&table.pstate);
    let build_barrier = &pstate.build_barrier;
    debug_assert!(build_barrier.phase() >= PHJ_BUILD_ALLOCATE);
    let mut phase = build_barrier.phase();
    if phase == PHJ_BUILD_ALLOCATE {
        build_barrier.arrive_and_wait()?;
        phase = PHJ_BUILD_HASH_INNER;
    }
    if phase == PHJ_BUILD_HASH_INNER {
        // If late, first help finish any growth in progress (C).
        if grow_batches_phase(pstate.grow_batches_barrier.attach()) != PHJ_GROW_BATCHES_ELECT {
            exec_parallel_hash_increase_num_batches(table)?;
        }
        if grow_buckets_phase(pstate.grow_buckets_barrier.attach()) != PHJ_GROW_BUCKETS_ELECT {
            exec_parallel_hash_increase_num_buckets(table)?;
        }
        ensure_batch_accessors(table)?;
        exec_parallel_hash_table_set_current_batch(table, 0);
        return Ok(true); // caller runs the insert loop, then _finish
    }
    Ok(false)
}

/// The tail of `MultiExecParallelHash` after the insert loop (or immediately,
/// when `_begin` returned false).
pub fn multi_exec_parallel_hash_finish<'mcx>(
    table: &mut ParallelHashJoinTable<'mcx>,
    ran_inner: bool,
) -> PgResult<()> {
    let pstate = Arc::clone(&table.pstate);
    let build_barrier = &pstate.build_barrier;
    if ran_inner {
        for i in 0..table.nbatch {
            table.batches[i as usize].inner_tuples.end_write()?;
        }
        exec_parallel_hash_merge_counters(table);
        pstate.grow_buckets_barrier.detach();
        pstate.grow_batches_barrier.detach();
        if build_barrier.arrive_and_wait()? {
            // Elected: batches are now fixed.
            pstate.locked().growth = ParallelHashGrowth::Disabled;
        }
    }

    table.curbatch = -1;
    {
        let g = pstate.locked();
        table.nbuckets = g.nbuckets;
        table.log2_nbuckets = g.nbuckets.trailing_zeros();
        table.total_tuples = g.total_tuples as f64;
    }
    if build_barrier.phase() < PHJ_BUILD_FREE {
        ensure_batch_accessors(table)?;
    }
    debug_assert!(matches!(
        build_barrier.phase(),
        PHJ_BUILD_HASH_OUTER | PHJ_BUILD_RUN | PHJ_BUILD_FREE
    ));
    Ok(())
}

// Copy a minimal-tuple image into the current chunk (dense_alloc analog) and
// return the header; None = the caller must retry (growth interposed).
fn parallel_tuple_alloc(
    table: &mut ParallelHashJoinTable<'_>,
    size: usize,
) -> PgResult<Option<*mut HashJoinTupleHdr>> {
    let pstate = Arc::clone(&table.pstate);
    let size = (size + 7) & !7;
    let curbatch = table.curbatch;

    // Fast path: room in this backend's chunk, no locking (C).
    let chunk = table.current_chunk;
    if !chunk.is_null() && size <= HASH_CHUNK_THRESHOLD {
        // SAFETY: current_chunk is this thread's own chunk.
        unsafe {
            if (*chunk).maxlen - (*chunk).used >= size {
                let result = tuple_at(chunk, (*chunk).used);
                (*chunk).used += size;
                debug_assert!((*chunk).used <= (*chunk).maxlen);
                return Ok(Some(result.cast()));
            }
        }
    }

    // Slow path: allocate a new chunk under the pstate lock.
    let mut g = pstate.locked();
    if g.growth == ParallelHashGrowth::NeedMoreBatches
        || g.growth == ParallelHashGrowth::NeedMoreBuckets
    {
        let growth = g.growth;
        table.current_chunk = core::ptr::null_mut();
        drop(g);
        if growth == ParallelHashGrowth::NeedMoreBatches {
            exec_parallel_hash_increase_num_batches(table)?;
        } else {
            exec_parallel_hash_increase_num_buckets(table)?;
        }
        return Ok(None);
    }

    let chunk_size = if size > HASH_CHUNK_THRESHOLD {
        size + HASH_CHUNK_HEADER_SIZE
    } else {
        HASH_CHUNK_SIZE
    };

    if g.growth != ParallelHashGrowth::Disabled {
        debug_assert!(curbatch == 0);
        debug_assert!(pstate.build_barrier.phase() == PHJ_BUILD_HASH_INNER);
        let gen = Arc::clone(table.batches_gen.as_ref().expect("batches installed"));
        // Space limit: always allow each backend at least one chunk.
        if table.batches[0].at_least_one_chunk {
            let mut b = gen[0].lock();
            if b.size + chunk_size > g.space_allowed {
                g.growth = ParallelHashGrowth::NeedMoreBatches;
                b.space_exhausted = true;
                return Ok(None);
            }
        }
        // Load factor.
        if table.nbatch == 1 {
            let mut b = gen[0].lock();
            b.ntuples += table.batches[0].ntuples;
            table.batches[0].ntuples = 0;
            if b.ntuples + 1 > table.nbuckets as usize * NTUP_PER_BUCKET
                && table.nbuckets < (i32::MAX as u32 / 2)
                && (table.nbuckets as usize) * 2 <= MAX_ALLOC_SIZE / SIZEOF_BUCKET
            {
                g.growth = ParallelHashGrowth::NeedMoreBuckets;
                return Ok(None);
            }
        }
    }

    let chunk = alloc_chunk(chunk_size);
    {
        let batch = table.shared_batch(curbatch);
        let mut b = batch.lock();
        b.size += chunk_size;
        // SAFETY: fresh chunk; list head guarded by the batch lock.
        unsafe {
            (*chunk).next = b.chunks.0;
        }
        b.chunks = SendPtr(chunk);
    }
    table.batches[curbatch as usize].at_least_one_chunk = true;
    // SAFETY: fresh chunk owned by this thread until published.
    unsafe {
        (*chunk).used = size;
    }
    if size <= HASH_CHUNK_THRESHOLD {
        table.current_chunk = chunk;
    }
    drop(g);
    // SAFETY: offset 0 of a fresh chunk.
    Ok(Some(unsafe { tuple_at(chunk, 0) }))
}

// Write header + image into `dst` and clear the match flag (C's insert body).
//
// SAFETY: dst points at >= HJTUPLE_OVERHEAD + image.len() writable bytes.
unsafe fn install_tuple(dst: *mut HashJoinTupleHdr, hashvalue: u32, image: &[u8]) {
    // SAFETY: caller contract.
    unsafe {
        (*dst).set_next(core::ptr::null_mut());
        (*dst).set_hashvalue(hashvalue);
        let mt = dst.cast::<u8>().add(HJTUPLE_OVERHEAD);
        core::ptr::copy_nonoverlapping(image.as_ptr(), mt, image.len());
        (*mt.cast::<MinimalTupleData>()).clear_match();
    }
}

/// `ExecParallelHashTableInsert`: build-phase insert; routes to memory
/// (batch 0) or a batch's shared tuplestore.
pub fn exec_parallel_hash_table_insert(
    table: &mut ParallelHashJoinTable<'_>,
    image: &[u8],
    hashvalue: u32,
) -> PgResult<()> {
    loop {
        let (bucketno, batchno) = table.get_bucket_and_batch(hashvalue);
        if batchno == 0 {
            debug_assert!(table.pstate.build_barrier.phase() == PHJ_BUILD_HASH_INNER);
            let Some(tuple) = parallel_tuple_alloc(table, HJTUPLE_OVERHEAD + image.len())? else {
                continue;
            };
            // SAFETY: parallel_tuple_alloc sized the slot for this image.
            unsafe { install_tuple(tuple, hashvalue, image) };
            table.push_tuple(bucketno, tuple);
        } else {
            let tuple_size = (HJTUPLE_OVERHEAD + image.len() + 7) & !7;
            debug_assert!(batchno > 0);
            if table.batches[batchno as usize].preallocated < tuple_size
                && !exec_parallel_hash_tuple_prealloc(table, batchno, tuple_size)?
            {
                continue;
            }
            debug_assert!(table.batches[batchno as usize].preallocated >= tuple_size);
            table.batches[batchno as usize].preallocated -= tuple_size;
            let a = &mut table.batches[batchno as usize];
            a.inner_tuples.put_tuple(&hashvalue.to_ne_bytes(), image)?;
        }
        table.batches[batchno as usize].ntuples += 1;
        return Ok(());
    }
}

/// `ExecParallelHashTableInsertCurrentBatch`: reload-phase insert; growth is
/// disabled, the tuple belongs here.
pub fn exec_parallel_hash_table_insert_current_batch(
    table: &mut ParallelHashJoinTable<'_>,
    image: &[u8],
    hashvalue: u32,
) -> PgResult<()> {
    let (bucketno, batchno) = table.get_bucket_and_batch(hashvalue);
    debug_assert!(batchno == table.curbatch);
    let tuple = parallel_tuple_alloc(table, HJTUPLE_OVERHEAD + image.len())?
        .expect("growth disabled during batch load");
    // SAFETY: parallel_tuple_alloc sized the slot for this image.
    unsafe { install_tuple(tuple, hashvalue, image) };
    table.push_tuple(bucketno, tuple);
    Ok(())
}

// ExecParallelHashTuplePrealloc.
fn exec_parallel_hash_tuple_prealloc(
    table: &mut ParallelHashJoinTable<'_>,
    batchno: i32,
    size: usize,
) -> PgResult<bool> {
    let pstate = Arc::clone(&table.pstate);
    let want = size.max(HASH_CHUNK_SIZE - HASH_CHUNK_HEADER_SIZE);
    debug_assert!(batchno > 0 && batchno < table.nbatch);
    debug_assert!(size == (size + 7) & !7);

    let mut g = pstate.locked();
    if g.growth == ParallelHashGrowth::NeedMoreBatches
        || g.growth == ParallelHashGrowth::NeedMoreBuckets
    {
        let growth = g.growth;
        drop(g);
        if growth == ParallelHashGrowth::NeedMoreBatches {
            exec_parallel_hash_increase_num_batches(table)?;
        } else {
            exec_parallel_hash_increase_num_buckets(table)?;
        }
        return Ok(false);
    }

    let batch = table.shared_batch(batchno);
    let mut b = batch.lock();
    if g.growth != ParallelHashGrowth::Disabled
        && table.batches[batchno as usize].at_least_one_chunk
        && b.estimated_size + want + HASH_CHUNK_HEADER_SIZE > g.space_allowed
    {
        // This batch would exceed the budget when loaded: repartition.
        b.space_exhausted = true;
        g.growth = ParallelHashGrowth::NeedMoreBatches;
        return Ok(false);
    }
    b.estimated_size += want + HASH_CHUNK_HEADER_SIZE;
    drop(b);
    drop(g);
    table.batches[batchno as usize].at_least_one_chunk = true;
    table.batches[batchno as usize].preallocated = want;
    Ok(true)
}

// ExecParallelHashMergeCounters.
fn exec_parallel_hash_merge_counters(table: &mut ParallelHashJoinTable<'_>) {
    let pstate = Arc::clone(&table.pstate);
    let mut g = pstate.locked();
    g.total_tuples = 0;
    for i in 0..table.nbatch {
        let a = &mut table.batches[i as usize];
        let batch = &table.batches_gen.as_ref().expect("batches installed")[i as usize];
        let mut b = batch.lock();
        b.size += a.size;
        b.estimated_size += a.estimated_size;
        b.ntuples += a.ntuples;
        b.old_ntuples += a.old_ntuples;
        a.size = 0;
        a.estimated_size = 0;
        a.ntuples = 0;
        a.old_ntuples = 0;
        g.total_tuples += b.ntuples;
    }
}

// ExecParallelHashPopChunkQueue.
fn pop_chunk_queue(table: &ParallelHashJoinTable<'_>) -> *mut HashMemoryChunkHdr {
    let mut g = table.pstate.locked();
    let chunk = g.chunk_work_queue.0;
    if !chunk.is_null() {
        // SAFETY: queue entries are live chunks; the head hand-off is
        // serialized by the pstate lock.
        g.chunk_work_queue = SendPtr(unsafe { (*chunk).next });
    }
    chunk
}

fn cfi() -> PgResult<()> {
    if init_small::globals::InterruptPending() {
        return postgres_seams::check_for_interrupts::call();
    }
    Ok(())
}

/// `ExecParallelHashIncreaseNumBatches`.
pub fn exec_parallel_hash_increase_num_batches(
    table: &mut ParallelHashJoinTable<'_>,
) -> PgResult<()> {
    let pstate = Arc::clone(&table.pstate);
    debug_assert!(pstate.build_barrier.phase() == PHJ_BUILD_HASH_INNER);

    let barrier = &pstate.grow_batches_barrier;
    let mut phase = grow_batches_phase(barrier.phase());
    if phase == PHJ_GROW_BATCHES_ELECT {
        if barrier.arrive_and_wait()? {
            let (old_gen, old_nbatch, new_nbatch) = {
                let mut g = pstate.locked();
                let old_gen = g.batches.take().expect("batches installed");
                g.old_batches = Some(Arc::clone(&old_gen));
                g.old_nbatch = table.nbatch;
                let new_nbatch = if table.nbatch == 1 {
                    // Single->multi: per-worker budget; two batches per
                    // participant (C's insufficiency argument).
                    g.space_allowed = get_hash_memory_limit();
                    (pstate.nparticipants as u32 * 2).next_power_of_two() as i32
                } else {
                    table.nbatch * 2
                };
                (old_gen, g.old_nbatch, new_nbatch)
            };
            close_batch_accessors(table)?;
            setup_batches(table, new_nbatch);
            {
                let mut g = pstate.locked();
                debug_assert!(g.nbatch == new_nbatch);
                let old_batch0 = &old_gen[0];
                let new_batch0 = &table.batches_gen.as_ref().expect("just set")[0];
                if old_nbatch == 1 {
                    // One large batch -> many smaller: shrink buckets too.
                    let old_ntuples = old_batch0.lock().ntuples;
                    let dtuples = (old_ntuples as f64 * 2.0) / new_nbatch as f64;
                    let max_buckets = prevpower2_u32((MAX_ALLOC_SIZE / SIZEOF_BUCKET) as u32);
                    let mut dbuckets = (dtuples / NTUP_PER_BUCKET as f64).ceil();
                    dbuckets = dbuckets.min(max_buckets as f64);
                    let mut new_nbuckets = dbuckets as u32;
                    new_nbuckets = new_nbuckets.max(1024);
                    new_nbuckets = new_nbuckets.next_power_of_two();
                    old_batch0.lock().buckets = None;
                    new_batch0.lock().buckets = Some(alloc_buckets(new_nbuckets));
                    g.nbuckets = new_nbuckets;
                } else {
                    let recycled = old_batch0.lock().buckets.take().expect("batch 0 buckets");
                    for slot in recycled.iter() {
                        slot.store(core::ptr::null_mut(), Ordering::Relaxed);
                    }
                    new_batch0.lock().buckets = Some(recycled);
                }
                let chunks = {
                    let mut b = old_batch0.lock();
                    core::mem::replace(&mut b.chunks, SendPtr(core::ptr::null_mut()))
                };
                g.chunk_work_queue = chunks;
                g.growth = ParallelHashGrowth::Disabled;
            }
        } else {
            close_batch_accessors(table)?;
        }
        phase = 1; // PHJ_GROW_BATCHES_REALLOCATE
    }
    if phase == 1 {
        barrier.arrive_and_wait()?;
        phase = 2; // PHJ_GROW_BATCHES_REPARTITION
    }
    if phase == 2 {
        ensure_batch_accessors(table)?;
        exec_parallel_hash_table_set_current_batch(table, 0);
        repartition_first(table)?;
        repartition_rest(table)?;
        exec_parallel_hash_merge_counters(table);
        barrier.arrive_and_wait()?;
        phase = 3; // PHJ_GROW_BATCHES_DECIDE
    }
    if phase == 3 {
        if barrier.arrive_and_wait()? {
            let mut space_exhausted = false;
            let mut extreme_skew_detected = false;
            ensure_batch_accessors(table)?;
            exec_parallel_hash_table_set_current_batch(table, 0);
            {
                let g = pstate.locked();
                let old_gen = g.old_batches.as_ref().expect("old generation present");
                let gen = g.batches.as_ref().expect("batches installed");
                let old_nbatch = g.old_nbatch;
                for i in 0..table.nbatch {
                    let parent = (i % old_nbatch) as usize;
                    // Sequential locking: parent may equal i (non-reentrant
                    // Mutex); this phase is barrier-exclusive anyway.
                    let (b_exhausted, b_estimated, b_ntuples, b_old_ntuples) = {
                        let b = gen[i as usize].lock();
                        (b.space_exhausted, b.estimated_size, b.ntuples, b.old_ntuples)
                    };
                    if b_exhausted || b_estimated > g.space_allowed {
                        space_exhausted = true;
                    }
                    let old_exhausted = old_gen[parent].lock().space_exhausted;
                    if old_exhausted || b_estimated > g.space_allowed {
                        // A child holding ALL of its parent's tuples means
                        // repartitioning cannot help (identical hash values).
                        let parent_old_ntuples = if parent == i as usize {
                            b_old_ntuples
                        } else {
                            gen[parent].lock().old_ntuples
                        };
                        if b_ntuples == parent_old_ntuples {
                            extreme_skew_detected = true;
                        }
                    }
                }
            }
            let mut g = pstate.locked();
            if extreme_skew_detected || table.nbatch >= i32::MAX / 2 {
                g.growth = ParallelHashGrowth::Disabled;
            } else if space_exhausted {
                g.growth = ParallelHashGrowth::NeedMoreBatches;
            } else {
                g.growth = ParallelHashGrowth::Ok;
            }
            // Free the old generation (chunks already consumed by the work
            // queue walkers; batch 0's bucket array taken above).
            let old = g.old_batches.take();
            drop(g);
            drop(old);
        }
        phase = 4; // PHJ_GROW_BATCHES_FINISH
    }
    if phase == 4 {
        barrier.arrive_and_wait()?;
    }
    Ok(())
}

// ExecParallelHashRepartitionFirst: batch 0's chunks -> new batches.
fn repartition_first(table: &mut ParallelHashJoinTable<'_>) -> PgResult<()> {
    debug_assert!(table.nbatch == table.pstate.locked().nbatch);
    loop {
        let chunk = pop_chunk_queue(table);
        if chunk.is_null() {
            break;
        }
        let mut idx = 0usize;
        // SAFETY: the chunk came off the work queue: this thread owns it.
        unsafe {
            while idx < (*chunk).used {
                let hash_tuple = tuple_at(chunk, idx);
                let hashvalue = (*hash_tuple).hashvalue();
                let mt = HashJoinTupleHdr::mintuple(hash_tuple);
                let t_len = (*mt.as_ptr()).t_len as usize;
                let image = core::slice::from_raw_parts(mt.as_ptr().cast::<u8>(), t_len);
                let (bucketno, batchno) = table.get_bucket_and_batch(hashvalue);
                debug_assert!(batchno < table.nbatch);
                if batchno == 0 {
                    let copy = parallel_tuple_alloc(table, HJTUPLE_OVERHEAD + t_len)?
                        .expect("growth disabled while repartitioning");
                    install_tuple(copy, hashvalue, image);
                    table.push_tuple(bucketno, copy);
                } else {
                    let tuple_size = (HJTUPLE_OVERHEAD + t_len + 7) & !7;
                    table.batches[batchno as usize].estimated_size += tuple_size;
                    table.batches[batchno as usize]
                        .inner_tuples
                        .put_tuple(&hashvalue.to_ne_bytes(), image)?;
                }
                table.batches[0].old_ntuples += 1;
                table.batches[batchno as usize].ntuples += 1;
                idx += (HJTUPLE_OVERHEAD + t_len + 7) & !7;
            }
            free_chunk(chunk);
        }
        cfi()?;
    }
    Ok(())
}

// ExecParallelHashRepartitionRest: old batches 1..n -> new batches.
fn repartition_rest(table: &mut ParallelHashJoinTable<'_>) -> PgResult<()> {
    let pstate = Arc::clone(&table.pstate);
    let (old_gen, old_nbatch) = {
        let g = pstate.locked();
        (Arc::clone(g.old_batches.as_ref().expect("old generation present")), g.old_nbatch)
    };
    let mut old_inner: Vec<SharedTuplestoreAccessor<'_>> = (1..old_nbatch)
        .map(|i| {
            SharedTuplestoreAccessor::attach(
                Arc::clone(&old_gen[i as usize].inner_tuples),
                Arc::clone(&pstate.fileset),
                table.participant,
                table.mcx,
            )
        })
        .collect();

    for (i, old) in old_inner.iter_mut().enumerate() {
        let old_batchno = i as i32 + 1;
        old.begin_parallel_scan()?;
        let mut meta = [0u8; 4];
        while let Some(mt) = old.parallel_scan_next(&mut meta)? {
            let hashvalue = u32::from_ne_bytes(meta);
            // SAFETY: sts image is valid until the next scan call; we copy
            // out (put_tuple) before advancing.
            let (image, t_len) = unsafe {
                let t_len = (*mt.as_ptr()).t_len as usize;
                (core::slice::from_raw_parts(mt.as_ptr().cast::<u8>(), t_len), t_len)
            };
            let tuple_size = (HJTUPLE_OVERHEAD + t_len + 7) & !7;
            let (_bucketno, batchno) = table.get_bucket_and_batch(hashvalue);
            table.batches[batchno as usize].estimated_size += tuple_size;
            table.batches[batchno as usize].ntuples += 1;
            table.batches[old_batchno as usize].old_ntuples += 1;
            table.batches[batchno as usize]
                .inner_tuples
                .put_tuple(&hashvalue.to_ne_bytes(), image)?;
            cfi()?;
        }
        old.end_parallel_scan()?;
    }
    Ok(())
}

/// `ExecParallelHashIncreaseNumBuckets`.
pub fn exec_parallel_hash_increase_num_buckets(
    table: &mut ParallelHashJoinTable<'_>,
) -> PgResult<()> {
    let pstate = Arc::clone(&table.pstate);
    debug_assert!(pstate.build_barrier.phase() == PHJ_BUILD_HASH_INNER);

    let barrier = &pstate.grow_buckets_barrier;
    let mut phase = grow_buckets_phase(barrier.phase());
    if phase == PHJ_GROW_BUCKETS_ELECT {
        if barrier.arrive_and_wait()? {
            let mut g = pstate.locked();
            g.nbuckets *= 2;
            let nbuckets = g.nbuckets;
            let size = nbuckets as usize * SIZEOF_BUCKET;
            let batch0 = &g.batches.as_ref().expect("batches installed")[0];
            let mut b = batch0.lock();
            b.size += size / 2;
            b.buckets = Some(alloc_buckets(nbuckets));
            let chunks = core::mem::replace(&mut b.chunks, SendPtr(core::ptr::null_mut()));
            drop(b);
            g.chunk_work_queue = chunks;
            g.growth = ParallelHashGrowth::Ok;
        }
        phase = 1; // PHJ_GROW_BUCKETS_REALLOCATE
    }
    if phase == 1 {
        barrier.arrive_and_wait()?;
        phase = 2; // PHJ_GROW_BUCKETS_REINSERT
    }
    if phase == 2 {
        ensure_batch_accessors(table)?;
        exec_parallel_hash_table_set_current_batch(table, 0);
        loop {
            let chunk = pop_chunk_queue(table);
            if chunk.is_null() {
                break;
            }
            let mut idx = 0usize;
            // SAFETY: this thread owns the popped chunk; entries are packed
            // HashJoinTuple images.
            unsafe {
                while idx < (*chunk).used {
                    let hash_tuple = tuple_at(chunk, idx);
                    let hashvalue = (*hash_tuple).hashvalue();
                    let (bucketno, batchno) = table.get_bucket_and_batch(hashvalue);
                    debug_assert!(batchno == 0);
                    table.push_tuple(bucketno, hash_tuple);
                    let t_len = (*HashJoinTupleHdr::mintuple(hash_tuple).as_ptr()).t_len as usize;
                    idx += (HJTUPLE_OVERHEAD + t_len + 7) & !7;
                }
                // Keep the chunk: its tuples are now re-linked. Re-chain it
                // onto batch 0 so freeing still finds it.
                let batch0 = table.shared_batch(0);
                let mut b = batch0.lock();
                (*chunk).next = b.chunks.0;
                b.chunks = SendPtr(chunk);
            }
            cfi()?;
        }
        barrier.arrive_and_wait()?;
    }
    Ok(())
}

/// `ExecHashTableDetachBatch`.
pub fn exec_hash_table_detach_batch(table: &mut ParallelHashJoinTable<'_>) -> PgResult<()> {
    if table.curbatch < 0 {
        return Ok(());
    }
    let curbatch = table.curbatch;
    table.batches[curbatch as usize].inner_tuples.end_parallel_scan()?;
    table.batches[curbatch as usize].outer_tuples.end_parallel_scan()?;
    let batch = &table.batches_gen.as_ref().expect("batches installed")[curbatch as usize];
    let barrier = &batch.batch_barrier;
    debug_assert!(matches!(barrier.phase(), PHJ_BATCH_PROBE | PHJ_BATCH_SCAN));

    // Early probe abandon = incomplete match bits: skip the unmatched scan.
    if barrier.phase() == PHJ_BATCH_PROBE && !table.batches[curbatch as usize].outer_eof {
        batch.skip_unmatched.store(true, Ordering::Relaxed);
    }

    let mut attached = true;
    if barrier.phase() == PHJ_BATCH_PROBE {
        attached = barrier.arrive_and_detach_except_last();
    }
    if attached && barrier.arrive_and_detach() {
        debug_assert!(barrier.phase() == PHJ_BATCH_FREE);
        let mut b = batch.lock();
        let mut chunk = b.chunks.0;
        while !chunk.is_null() {
            // SAFETY: sole owner at PHJ_BATCH_FREE; list traversal then free.
            unsafe {
                let next = (*chunk).next;
                free_chunk(chunk);
                chunk = next;
            }
        }
        b.chunks = SendPtr(core::ptr::null_mut());
        b.buckets = None;
    }

    let size = batch.lock().size;
    table.space_peak =
        table.space_peak.max(size + SIZEOF_BUCKET * table.nbuckets as usize);
    table.curbatch = -1;
    Ok(())
}

/// `ExecParallelPrepHashTableForUnmatched`'s bookkeeping when losing the
/// election (the caller handles the state machine transitions).
pub fn parallel_prep_unmatched_lose(table: &mut ParallelHashJoinTable<'_>) -> PgResult<()> {
    let curbatch = table.curbatch;
    table.batches[curbatch as usize].done = true;
    table.batches[curbatch as usize].inner_tuples.end_parallel_scan()?;
    table.batches[curbatch as usize].outer_tuples.end_parallel_scan()?;
    let size = table.shared_batch(curbatch).lock().size;
    table.space_peak =
        table.space_peak.max(size + SIZEOF_BUCKET * table.nbuckets as usize);
    table.curbatch = -1;
    Ok(())
}

/// `ExecHashTableDetach`.
pub fn exec_hash_table_detach(table: &mut ParallelHashJoinTable<'_>) -> PgResult<()> {
    if table.detached {
        return Ok(());
    }
    table.detached = true;
    let pstate = Arc::clone(&table.pstate);
    debug_assert!(pstate.build_barrier.phase() >= PHJ_BUILD_RUN);
    if pstate.build_barrier.phase() == PHJ_BUILD_RUN {
        close_batch_accessors(table)?;
        if pstate.build_barrier.arrive_and_detach() {
            debug_assert!(pstate.build_barrier.phase() == PHJ_BUILD_FREE);
            let mut g = pstate.locked();
            let gen = g.batches.take();
            drop(g);
            if let Some(gen) = gen {
                free_generation(&gen);
            }
        }
    } else if !table.batches.is_empty() {
        close_batch_accessors(table)?;
    }
    Ok(())
}

// Give-up path for skipped batches (normal frees happen at PHJ_BATCH_FREE).
fn free_generation(gen: &Arc<[ParallelHashJoinBatch]>) {
    for batch in gen.iter() {
        let mut b = batch.lock();
        let mut chunk = b.chunks.0;
        while !chunk.is_null() {
            // SAFETY: last participant; exclusive owner.
            unsafe {
                let next = (*chunk).next;
                free_chunk(chunk);
                chunk = next;
            }
        }
        b.chunks = SendPtr(core::ptr::null_mut());
        b.buckets = None;
    }
}

impl Drop for BatchShared {
    fn drop(&mut self) {
        // Error-path backstop only.
        let mut chunk = self.chunks.0;
        while !chunk.is_null() {
            // SAFETY: dropping the generation Arc means no accessor remains.
            unsafe {
                let next = (*chunk).next;
                free_chunk(chunk);
                chunk = next;
            }
        }
    }
}

fn prevpower2_u32(n: u32) -> u32 {
    if n == 0 {
        1
    } else {
        1 << (31 - n.leading_zeros())
    }
}

/// Fetch the current slot contents as a minimal-tuple image for insertion
/// (shared arms copy the bytes, so the scratch lifetime is per-call).
pub fn slot_min_tuple_image<'a>(
    estate: &'a mut EStateData<'_>,
    slot_id: ExecSlotId,
    ecxt: EcxtId,
) -> PgResult<(*const u8, usize)> {
    let query_mcx = estate.es_query_cxt;
    let (slot, scratch_mcx) = estate.slot_and_per_tuple_mcx(slot_id, ecxt);
    let fetched = exectuples::exec_fetch_slot_minimal_tuple(slot, query_mcx, scratch_mcx)?;
    let (ptr, t_len): (*const u8, u32) = match &fetched {
        exectuples::FetchedMinimalTuple::Slot(m, _) => {
            // SAFETY: live stored image; header read.
            (m.as_ptr().cast_const().cast(), unsafe { m.as_ref().t_len })
        }
        exectuples::FetchedMinimalTuple::Copied(t) => (t.as_ptr(), t.t_len()),
    };
    Ok((ptr, t_len as usize))
}
