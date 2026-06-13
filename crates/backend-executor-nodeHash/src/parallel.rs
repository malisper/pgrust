//! The Parallel Hash Join shared-memory machinery: DSA-resident bucket arrays
//! and chunk lists, the build/grow/repartition coordination across the
//! barriers, the per-batch accessors, and the shared-tuple insert/scan paths.
//!
//! The genuinely-shared structures (`ParallelHashJoinState`,
//! `ParallelHashJoinBatch`, the dense-allocation `HashMemoryChunkData` headers,
//! and the per-tuple `HashJoinTupleData` headers) live in the one MAP_SHARED
//! DSA area, reached by `dsa_pointer` and resolved with `dsa_get_address`
//! exactly as in C. The dsa owner's `dsa_get_address` seam returns a
//! `SerializeCursor` whose `.0` is the resolved byte address of the addressed
//! object (the faithful analogue of C's `dsa_get_address` returning a real
//! pointer); the typed accessors below reinterpret it. The barrier / LWLock /
//! shared-tuplestore / DSA-allocator operations are reached through their
//! owners' seam crates. The lock-free bucket-chain CAS push
//! (`dsa_pointer_atomic_compare_exchange`) is this unit's own logic over the
//! resolved `pg_atomic_uint64` word, mirroring C.

use mcx::Mcx;
use types_core::{Size, uint32};
use types_error::PgResult;
use types_execparallel::{dsa_pointer_is_valid, DsaPointer, INVALID_DSA_POINTER};
use types_nodes::nodehash::{
    HashChunkIdx, HashJoinState, HashJoinTableData, HashTupleIdx, ParallelHashGrowth,
    ParallelHashJoinBatch, ParallelHashJoinState, PHJ_BUILD_HASH_INNER, PHJ_GROW_BATCHES_DECIDE,
    PHJ_GROW_BATCHES_ELECT, PHJ_GROW_BATCHES_FINISH, PHJ_GROW_BATCHES_PHASE,
    PHJ_GROW_BATCHES_REALLOCATE, PHJ_GROW_BATCHES_REPARTITION, PHJ_GROW_BUCKETS_ELECT,
    PHJ_GROW_BUCKETS_PHASE, PHJ_GROW_BUCKETS_REALLOCATE,
};

use core::sync::atomic::{AtomicU64, Ordering};

use backend_storage_ipc_barrier_seams as barrier;
use backend_storage_lmgr_lwlock_seams as lwlock;
use backend_utils_mmgr_dsa_seams as dsa;
use backend_utils_sort_storage_seams as sts;
use types_storage::{pg_atomic_uint64, LWLockMode};

use crate::{HASH_CHUNK_HEADER_SIZE, HJTUPLE_OVERHEAD, MAXALIGN, MaxAllocSize};
use types_nodes::nodehash::{HASH_CHUNK_SIZE, HASH_CHUNK_THRESHOLD};

// ===========================================================================
//   File-owned constants & arithmetic (hashjoin.h / pg_bitutils.h / wait_event.h)
// ===========================================================================

/// `NTUP_PER_BUCKET` (nodeHash.c:655).
const NTUP_PER_BUCKET: i32 = 1;

/// `sizeof(dsa_pointer_atomic)` — 8 bytes on the 64-bit ABI.
const SIZEOF_DSA_POINTER_ATOMIC: usize = 8;

/// `INT_MAX` (limits.h).
const INT_MAX: i32 = i32::MAX;

// `SizeofMinimalTupleHeader` re-used; a `MinimalTuple`'s `t_len` lives at the
// first 4 bytes of the on-DSA image (htup_details.h: `MinimalTupleData.t_len`).

// ---- WAIT_EVENT_* (wait_event.h): the parallel-hash build/grow waits. ----
const WAIT_EVENT_HASH_GROW_BATCHES_ELECT: uint32 = 0x0A00_0006;
const WAIT_EVENT_HASH_GROW_BATCHES_REALLOCATE: uint32 = 0x0A00_0007;
const WAIT_EVENT_HASH_GROW_BATCHES_REPARTITION: uint32 = 0x0A00_0008;
const WAIT_EVENT_HASH_GROW_BATCHES_DECIDE: uint32 = 0x0A00_0009;
const WAIT_EVENT_HASH_GROW_BATCHES_FINISH: uint32 = 0x0A00_000A;
const WAIT_EVENT_HASH_GROW_BUCKETS_ELECT: uint32 = 0x0A00_000B;
const WAIT_EVENT_HASH_GROW_BUCKETS_REALLOCATE: uint32 = 0x0A00_000C;
const WAIT_EVENT_HASH_GROW_BUCKETS_REINSERT: uint32 = 0x0A00_000D;

/// `pg_nextpower2_32(num)` (pg_bitutils.h) — the next power of 2 >= num
/// (num must be in `[1, PG_UINT32_MAX/2 + 1]`).
#[inline]
fn pg_nextpower2_32(num: u32) -> u32 {
    if num <= 1 {
        return 1;
    }
    1u32 << (32 - (num - 1).leading_zeros())
}

/// `pg_prevpower2_32(num)` (pg_bitutils.h) — the previous power of 2 <= num.
#[inline]
fn pg_prevpower2_32(num: u32) -> u32 {
    1u32 << (31 - num.leading_zeros())
}

/// `my_log2(num)` (nodeHash.c) — `ceil(log2(num))`.
#[inline]
fn my_log2(num: i64) -> i32 {
    // pg_ceil_log2_64
    let mut i = 0i32;
    let mut limit = 1i64;
    while limit < num {
        limit <<= 1;
        i += 1;
    }
    i
}

// `EstimateParallelHashJoinBatch(hashtable)` (hashjoin.h:193) —
//   MAXALIGN(sizeof(ParallelHashJoinBatch)) +
//   MAXALIGN(sts_estimate(nparticipants)) * 2
// The `sts_estimate` term is reached through the shared-tuplestore owner.
#[inline]
fn estimate_parallel_hash_join_batch(nparticipants: i32) -> Size {
    MAXALIGN(core::mem::size_of::<ParallelHashJoinBatch>())
        + MAXALIGN(sts::sts_estimate::call(nparticipants)) * 2
}

// `ParallelHashJoinBatchInner(batch)` (hashjoin.h:182): the SharedTuplestore
// for inner tuples sits MAXALIGN(sizeof(ParallelHashJoinBatch)) bytes past the
// batch header. `ParallelHashJoinBatchOuter` is another MAXALIGN(sts_estimate)
// past that. These return DSA-resolved addresses (the sts owner wraps them).
#[inline]
fn parallel_hash_join_batch_inner(batch_addr: usize) -> types_execparallel::SharedTuplestoreHandle {
    types_execparallel::SharedTuplestoreHandle(
        batch_addr + MAXALIGN(core::mem::size_of::<ParallelHashJoinBatch>()),
    )
}

#[inline]
fn parallel_hash_join_batch_outer(
    batch_addr: usize,
    nparticipants: i32,
) -> types_execparallel::SharedTuplestoreHandle {
    types_execparallel::SharedTuplestoreHandle(
        batch_addr
            + MAXALIGN(core::mem::size_of::<ParallelHashJoinBatch>())
            + MAXALIGN(sts::sts_estimate::call(nparticipants)),
    )
}

// ===========================================================================
//   DSA-resident struct resolution. `dsa_get_address` (the dsa owner's seam)
//   resolves a dsa_pointer to a SerializeCursor whose `.0` is the addressed
//   object's byte address — the faithful analogue of C's `dsa_get_address`
//   returning a real pointer. We reinterpret it to the typed struct exactly as
//   the C casts do. Safety: the dsa owner guarantees the address is valid for
//   the lifetime the cohort is attached (C relies on the same invariant).
// ===========================================================================

#[inline]
fn dsa_get_addr(area: types_execparallel::DsaAreaHandle, dp: DsaPointer) -> usize {
    dsa::dsa_get_address::call(area, dp).0
}

/// Resolve `hashtable->parallel_state` to its DSA-resident struct. The returned
/// `&mut` aliases DSA memory the dsa owner keeps live; it does NOT borrow
/// `hashtable`, mirroring C where `pstate` is a raw pointer independent of the
/// `HashJoinTable`.
#[inline]
#[allow(clippy::mut_from_ref)]
fn pstate_mut<'a>(hashtable: &HashJoinTableData<'_>) -> &'a mut ParallelHashJoinState {
    let area = hashtable.area.expect("parallel hash: area is None");
    let ps = hashtable
        .parallel_state
        .expect("parallel hash: parallel_state is None");
    let addr = dsa_get_addr(area, ps);
    unsafe { &mut *(addr as *mut ParallelHashJoinState) }
}

#[inline]
#[allow(clippy::mut_from_ref)]
fn batch_shared_mut<'a>(
    hashtable: &HashJoinTableData<'_>,
    shared_dp: DsaPointer,
) -> &'a mut ParallelHashJoinBatch {
    let area = hashtable.area.expect("parallel hash: area is None");
    let addr = dsa_get_addr(area, shared_dp);
    unsafe { &mut *(addr as *mut ParallelHashJoinBatch) }
}

/// `NthParallelHashJoinBatch(base_addr, n)` (hashjoin.h:198): the nth shared
/// batch in the pseudo-array starting at `base_addr`, stride
/// `EstimateParallelHashJoinBatch`.
#[inline]
fn nth_batch_addr(base_addr: usize, n: i32, nparticipants: i32) -> usize {
    base_addr + estimate_parallel_hash_join_batch(nparticipants) * n as usize
}

#[inline]
#[allow(clippy::mut_from_ref)]
fn batch_at_addr<'a>(addr: usize) -> &'a mut ParallelHashJoinBatch {
    unsafe { &mut *(addr as *mut ParallelHashJoinBatch) }
}

/// The dsa-resolved bucket-head atomic array for the current batch
/// (`hashtable->buckets.shared`). C resolves it from
/// `batches[curbatch].shared->buckets`; we resolve the same dsa_pointer.
#[inline]
fn current_shared_buckets(hashtable: &HashJoinTableData<'_>) -> usize {
    let curbatch = hashtable.curbatch;
    let shared_dp = hashtable.batches[curbatch as usize].shared;
    let batch = batch_shared_mut(hashtable, shared_dp);
    let area = hashtable.area.expect("parallel hash: area is None");
    dsa_get_addr(area, batch.buckets)
}

/// `dsa_pointer_atomic_read(&buckets[bucketno])`.
#[inline]
fn bucket_atomic_read(buckets_addr: usize, bucketno: i32) -> DsaPointer {
    let p = (buckets_addr + bucketno as usize * SIZEOF_DSA_POINTER_ATOMIC) as *const AtomicU64;
    unsafe { (*p).load(Ordering::Acquire) }
}

/// The DSA-resident `HashMemoryChunkData` at the given dsa_pointer.
#[inline]
#[allow(clippy::mut_from_ref)]
fn chunk_at<'a>(hashtable: &HashJoinTableData<'_>, dp: DsaPointer) -> &'a mut HashMemoryChunkRaw {
    let area = hashtable.area.expect("parallel hash: area is None");
    let addr = dsa_get_addr(area, dp);
    unsafe { &mut *(addr as *mut HashMemoryChunkRaw) }
}

/// On-DSA `HashMemoryChunkData` header (hashjoin.h). The owned-arena
/// `HashMemoryChunkData` in types-nodes models the serial path; the parallel
/// path works on the genuine in-DSA header, whose `next` link is the `shared`
/// dsa_pointer. Layout mirrors the C struct's accounting + shared-next fields.
#[repr(C)]
struct HashMemoryChunkRaw {
    /// `union { ... dsa_pointer shared; } next` — next chunk's dsa_pointer.
    next_shared: DsaPointer,
    /// `size_t maxlen`.
    maxlen: Size,
    /// `size_t used`.
    used: Size,
    /// `int ntuples`.
    ntuples: i32,
}

/// On-DSA `HashJoinTupleData` header (hashjoin.h). The parallel path carves
/// these out of the chunk byte buffers and links chains by dsa_pointer.
#[repr(C)]
struct HashJoinTupleRaw {
    /// `union { ... dsa_pointer shared; } next`.
    next_shared: DsaPointer,
    /// `uint32 hashvalue`.
    hashvalue: uint32,
    // followed inline by the MinimalTuple image.
}

/// `HASH_CHUNK_DATA(chunk)` (hashjoin.h:152): the byte after the chunk header.
#[inline]
fn hash_chunk_data_addr(chunk_addr: usize) -> usize {
    chunk_addr + HASH_CHUNK_HEADER_SIZE
}

/// `HJTUPLE_MINTUPLE(hjtup)` (hashjoin.h:91): the MinimalTuple image, just past
/// the HashJoinTuple header.
#[inline]
fn hjtuple_mintuple_addr(hjtuple_addr: usize) -> usize {
    hjtuple_addr + HJTUPLE_OVERHEAD
}

/// `tuple->t_len` for an on-DSA MinimalTuple at `mintuple_addr`
/// (`MinimalTupleData.t_len` is the first u32 of the flat image).
#[inline]
fn mintuple_t_len(mintuple_addr: usize) -> usize {
    unsafe { *(mintuple_addr as *const u32) as usize }
}

// On-DSA MinimalTuple flat-image field offsets (htup_details.h):
//   t_len:u32 @0, mt_padding[6] @4, t_infomask2:u16 @10, t_infomask:u16 @12,
//   t_hoff:u8 @14. The match flag is HEAP_TUPLE_HAS_MATCH in t_infomask2.
const MT_OFF_INFOMASK2: usize = 10;
use types_tuple::heaptuple::HEAP_TUPLE_HAS_MATCH;

/// `HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(...))` over the on-DSA image —
/// the htup_details.h inline accessor (`t_infomask2 &= ~HEAP_TUPLE_HAS_MATCH`),
/// own bit logic over the flat tuple image this code stored.
#[inline]
fn heap_tuple_header_clear_match(mintuple_addr: usize) {
    let p = (mintuple_addr + MT_OFF_INFOMASK2) as *mut u16;
    unsafe {
        *p &= !HEAP_TUPLE_HAS_MATCH;
    }
}

/// `HeapTupleHeaderHasMatch(HJTUPLE_MINTUPLE(...))`
/// (`(t_infomask2 & HEAP_TUPLE_HAS_MATCH) != 0`).
#[inline]
fn heap_tuple_header_has_match(mintuple_addr: usize) -> bool {
    let p = (mintuple_addr + MT_OFF_INFOMASK2) as *const u16;
    unsafe { (*p & HEAP_TUPLE_HAS_MATCH) != 0 }
}

// ===========================================================================
//   ExecParallelHashIncreaseNumBatches (nodeHash.c:1198)
// ===========================================================================

/// `ExecParallelHashIncreaseNumBatches(HashJoinTable hashtable)` — coordinate
/// doubling the batch count across participants via the grow_batches_barrier
/// phase machine (ELECT→REALLOCATE→REPARTITION→DECIDE→FINISH).
pub fn ExecParallelHashIncreaseNumBatches<'mcx>(
    mcx: Mcx<'mcx>,
    hashtable: &mut HashJoinTableData<'mcx>,
) -> PgResult<()> {
    // Assert(BarrierPhase(&pstate->build_barrier) == PHJ_BUILD_HASH_INNER);
    {
        let pstate = pstate_mut(hashtable);
        debug_assert_eq!(
            barrier::BarrierPhase::call(&pstate.build_barrier),
            PHJ_BUILD_HASH_INNER
        );
    }

    let grow_phase = {
        let pstate = pstate_mut(hashtable);
        barrier::BarrierPhase::call(&pstate.grow_batches_barrier)
    };

    // switch (PHJ_GROW_BATCHES_PHASE(...)) — with C fall-through.
    let mut phase = PHJ_GROW_BATCHES_PHASE(grow_phase);

    if phase == PHJ_GROW_BATCHES_ELECT {
        let elected = {
            let pstate = pstate_mut(hashtable);
            barrier::BarrierArriveAndWait::call(
                &mut pstate.grow_batches_barrier,
                WAIT_EVENT_HASH_GROW_BATCHES_ELECT,
            )
        };
        if elected {
            ipnb_elect_reallocate(mcx, hashtable)?;
        } else {
            // All other participants just flush their tuples to disk.
            ExecParallelHashCloseBatchAccessors(hashtable)?;
        }
        phase = PHJ_GROW_BATCHES_REALLOCATE; // fall through
    }

    if phase == PHJ_GROW_BATCHES_REALLOCATE {
        let pstate = pstate_mut(hashtable);
        barrier::BarrierArriveAndWait::call(
            &mut pstate.grow_batches_barrier,
            WAIT_EVENT_HASH_GROW_BATCHES_REALLOCATE,
        );
        phase = PHJ_GROW_BATCHES_REPARTITION; // fall through
    }

    if phase == PHJ_GROW_BATCHES_REPARTITION {
        ExecParallelHashEnsureBatchAccessors(mcx, hashtable)?;
        ExecParallelHashTableSetCurrentBatch(hashtable, 0);
        ExecParallelHashRepartitionFirst(mcx, hashtable)?;
        ExecParallelHashRepartitionRest(mcx, hashtable)?;
        ExecParallelHashMergeCounters(hashtable);
        let pstate = pstate_mut(hashtable);
        barrier::BarrierArriveAndWait::call(
            &mut pstate.grow_batches_barrier,
            WAIT_EVENT_HASH_GROW_BATCHES_REPARTITION,
        );
        phase = PHJ_GROW_BATCHES_DECIDE; // fall through
    }

    if phase == PHJ_GROW_BATCHES_DECIDE {
        let elected = {
            let pstate = pstate_mut(hashtable);
            barrier::BarrierArriveAndWait::call(
                &mut pstate.grow_batches_barrier,
                WAIT_EVENT_HASH_GROW_BATCHES_DECIDE,
            )
        };
        if elected {
            ipnb_decide(mcx, hashtable)?;
        }
        phase = PHJ_GROW_BATCHES_FINISH; // fall through
    }

    if phase == PHJ_GROW_BATCHES_FINISH {
        let pstate = pstate_mut(hashtable);
        barrier::BarrierArriveAndWait::call(
            &mut pstate.grow_batches_barrier,
            WAIT_EVENT_HASH_GROW_BATCHES_FINISH,
        );
    }

    Ok(())
}

/// The PHJ_GROW_BATCHES_ELECT elected-participant body of
/// `ExecParallelHashIncreaseNumBatches`.
fn ipnb_elect_reallocate<'mcx>(
    mcx: Mcx<'mcx>,
    hashtable: &mut HashJoinTableData<'mcx>,
) -> PgResult<()> {
    let area = hashtable.area.expect("parallel hash: area is None");
    let nparticipants = pstate_mut(hashtable).nparticipants;

    // old_batch0 = hashtable->batches[0].shared;
    let old_batch0_dp = hashtable.batches[0].shared;
    let old_batch0_addr = dsa_get_addr(area, old_batch0_dp);

    // pstate->old_batches = pstate->batches; pstate->old_nbatch = nbatch;
    // pstate->batches = InvalidDsaPointer;
    {
        let pstate = pstate_mut(hashtable);
        pstate.old_batches = pstate.batches;
        pstate.old_nbatch = hashtable.nbatch;
        pstate.batches = INVALID_DSA_POINTER;
    }

    // Free this backend's old accessors.
    ExecParallelHashCloseBatchAccessors(hashtable)?;

    // Figure out how many batches to use.
    let new_nbatch = if hashtable.nbatch == 1 {
        // Single-batch to multi-batch: switch to the regular hash_mem budget.
        // C: pstate->space_allowed = get_hash_memory_limit(); the GUC values
        // (work_mem / hash_mem_multiplier) are read by the sibling helper from
        // backend GUC state, passed explicitly per the no-ambient-globals rule.
        let (work_mem, hash_mem_multiplier) = hash_mem_gucs();
        let limit = crate::hash_table::get_hash_memory_limit(work_mem, hash_mem_multiplier);
        pstate_mut(hashtable).space_allowed = limit;
        pg_nextpower2_32((nparticipants * 2) as u32) as i32
    } else {
        hashtable.nbatch * 2
    };

    // Allocate new larger generation of batches.
    debug_assert_eq!(hashtable.nbatch, pstate_mut(hashtable).nbatch);
    ExecParallelHashJoinSetUpBatches(mcx, hashtable, new_nbatch)?;
    debug_assert_eq!(hashtable.nbatch, pstate_mut(hashtable).nbatch);

    // Replace or recycle batch 0's bucket array.
    let old_nbatch = pstate_mut(hashtable).old_nbatch;
    if old_nbatch == 1 {
        let old_batch0 = batch_at_addr::<'_>(old_batch0_addr);
        let dtuples = (old_batch0.ntuples as f64 * 2.0) / new_nbatch as f64;
        let max_buckets =
            pg_prevpower2_32((MaxAllocSize / SIZEOF_DSA_POINTER_ATOMIC) as u32);
        let mut dbuckets = (dtuples / NTUP_PER_BUCKET as f64).ceil();
        dbuckets = dbuckets.min(max_buckets as f64);
        let mut new_nbuckets = dbuckets as i32;
        new_nbuckets = new_nbuckets.max(1024);
        new_nbuckets = pg_nextpower2_32(new_nbuckets as u32) as i32;
        let old_buckets_dp = old_batch0.buckets;
        dsa::dsa_free::call(area, old_buckets_dp);
        let new_buckets = dsa::dsa_allocate::call(
            area,
            SIZEOF_DSA_POINTER_ATOMIC * new_nbuckets as usize,
        );
        let batch0_dp = hashtable.batches[0].shared;
        batch_shared_mut(hashtable, batch0_dp).buckets = new_buckets;
        let buckets_addr = dsa_get_addr(area, new_buckets);
        for i in 0..new_nbuckets {
            dsa_pointer_atomic_init(buckets_addr, i, INVALID_DSA_POINTER);
        }
        pstate_mut(hashtable).nbuckets = new_nbuckets;
    } else {
        // Recycle the existing bucket array.
        let old_batch0 = batch_at_addr::<'_>(old_batch0_addr);
        let old_buckets_dp = old_batch0.buckets;
        let batch0_dp = hashtable.batches[0].shared;
        batch_shared_mut(hashtable, batch0_dp).buckets = old_buckets_dp;
        let buckets_addr = dsa_get_addr(area, old_buckets_dp);
        let nbuckets = hashtable.nbuckets;
        for i in 0..nbuckets {
            dsa_pointer_atomic_write(buckets_addr, i, INVALID_DSA_POINTER);
        }
    }

    // Move all chunks to the work queue for parallel processing.
    let old_batch0 = batch_at_addr::<'_>(old_batch0_addr);
    let old_chunks = old_batch0.chunks;
    {
        let pstate = pstate_mut(hashtable);
        pstate.chunk_work_queue = old_chunks;
        // Disable further growth temporarily while we're growing.
        pstate.growth = ParallelHashGrowth::PHJ_GROWTH_DISABLED;
    }

    Ok(())
}

/// The PHJ_GROW_BATCHES_DECIDE elected-participant body of
/// `ExecParallelHashIncreaseNumBatches`.
fn ipnb_decide<'mcx>(mcx: Mcx<'mcx>, hashtable: &mut HashJoinTableData<'mcx>) -> PgResult<()> {
    let area = hashtable.area.expect("parallel hash: area is None");

    ExecParallelHashEnsureBatchAccessors(mcx, hashtable)?;
    ExecParallelHashTableSetCurrentBatch(hashtable, 0);

    let (old_batches_dp, old_nbatch, space_allowed, nparticipants) = {
        let pstate = pstate_mut(hashtable);
        (
            pstate.old_batches,
            pstate.old_nbatch,
            pstate.space_allowed,
            pstate.nparticipants,
        )
    };
    let old_batches_addr = dsa_get_addr(area, old_batches_dp);

    let mut space_exhausted = false;
    let mut extreme_skew_detected = false;

    let nbatch = hashtable.nbatch;
    for i in 0..nbatch {
        let batch_dp = hashtable.batches[i as usize].shared;
        let (b_space_exhausted, b_estimated_size, b_ntuples) = {
            let batch = batch_shared_mut(hashtable, batch_dp);
            (batch.space_exhausted, batch.estimated_size, batch.ntuples)
        };
        if b_space_exhausted || b_estimated_size > space_allowed {
            space_exhausted = true;
        }

        let parent = i % old_nbatch;
        let old_batch =
            batch_at_addr::<'_>(nth_batch_addr(old_batches_addr, parent, nparticipants));
        if old_batch.space_exhausted || b_estimated_size > space_allowed {
            let parent_dp = hashtable.batches[parent as usize].shared;
            let parent_old_ntuples = batch_shared_mut(hashtable, parent_dp).old_ntuples;
            if b_ntuples == parent_old_ntuples {
                extreme_skew_detected = true;
            }
        }
    }

    {
        let pstate = pstate_mut(hashtable);
        if extreme_skew_detected || hashtable.nbatch >= INT_MAX / 2 {
            pstate.growth = ParallelHashGrowth::PHJ_GROWTH_DISABLED;
        } else if space_exhausted {
            pstate.growth = ParallelHashGrowth::PHJ_GROWTH_NEED_MORE_BATCHES;
        } else {
            pstate.growth = ParallelHashGrowth::PHJ_GROWTH_OK;
        }
        // Free the old batches in shared memory.
        dsa::dsa_free::call(area, pstate.old_batches);
        pstate.old_batches = INVALID_DSA_POINTER;
    }

    Ok(())
}

// ===========================================================================
//   ExecParallelHashIncreaseNumBuckets (nodeHash.c:1650)
// ===========================================================================

/// `ExecParallelHashIncreaseNumBuckets(HashJoinTable hashtable)` — coordinate
/// doubling the bucket count and reinserting via the grow_buckets_barrier
/// (ELECT→REALLOCATE→REINSERT).
pub fn ExecParallelHashIncreaseNumBuckets<'mcx>(
    mcx: Mcx<'mcx>,
    hashtable: &mut HashJoinTableData<'mcx>,
) -> PgResult<()> {
    {
        let pstate = pstate_mut(hashtable);
        debug_assert_eq!(
            barrier::BarrierPhase::call(&pstate.build_barrier),
            PHJ_BUILD_HASH_INNER
        );
    }

    let grow_phase = {
        let pstate = pstate_mut(hashtable);
        barrier::BarrierPhase::call(&pstate.grow_buckets_barrier)
    };
    let mut phase = PHJ_GROW_BUCKETS_PHASE(grow_phase);

    if phase == PHJ_GROW_BUCKETS_ELECT {
        let elected = {
            let pstate = pstate_mut(hashtable);
            barrier::BarrierArriveAndWait::call(
                &mut pstate.grow_buckets_barrier,
                WAIT_EVENT_HASH_GROW_BUCKETS_ELECT,
            )
        };
        if elected {
            ipnbk_elect(hashtable)?;
        }
        phase = PHJ_GROW_BUCKETS_REALLOCATE; // fall through
    }

    if phase == PHJ_GROW_BUCKETS_REALLOCATE {
        let pstate = pstate_mut(hashtable);
        barrier::BarrierArriveAndWait::call(
            &mut pstate.grow_buckets_barrier,
            WAIT_EVENT_HASH_GROW_BUCKETS_REALLOCATE,
        );
        phase = types_nodes::nodehash::PHJ_GROW_BUCKETS_REINSERT; // fall through
    }

    if phase == types_nodes::nodehash::PHJ_GROW_BUCKETS_REINSERT {
        ExecParallelHashEnsureBatchAccessors(mcx, hashtable)?;
        ExecParallelHashTableSetCurrentBatch(hashtable, 0);
        while let Some((_chunk_idx, chunk_s)) = ExecParallelHashPopChunkQueue(hashtable) {
            let area = hashtable.area.expect("parallel hash: area is None");
            let chunk_addr = dsa_get_addr(area, chunk_s);
            let chunk_used = chunk_at(hashtable, chunk_s).used;
            let mut idx: usize = 0;
            while idx < chunk_used {
                let hashtuple_addr = hash_chunk_data_addr(chunk_addr) + idx;
                let shared = chunk_s + HASH_CHUNK_HEADER_SIZE as u64 + idx as u64;
                let hashvalue =
                    unsafe { (*(hashtuple_addr as *const HashJoinTupleRaw)).hashvalue };
                let bb = exec_hash_get_bucket_and_batch(hashtable, hashvalue);
                debug_assert_eq!(bb.batchno, 0);
                ExecParallelHashPushTuple(hashtable, bb.bucketno, HashTupleIdx(hashtuple_addr), shared);
                let mintuple_addr = hjtuple_mintuple_addr(hashtuple_addr);
                idx += MAXALIGN(HJTUPLE_OVERHEAD + mintuple_t_len(mintuple_addr));
            }
            // CHECK_FOR_INTERRUPTS();
        }
        let pstate = pstate_mut(hashtable);
        barrier::BarrierArriveAndWait::call(
            &mut pstate.grow_buckets_barrier,
            WAIT_EVENT_HASH_GROW_BUCKETS_REINSERT,
        );
    }

    Ok(())
}

/// The PHJ_GROW_BUCKETS_ELECT elected-participant body.
fn ipnbk_elect(hashtable: &mut HashJoinTableData<'_>) -> PgResult<()> {
    let area = hashtable.area.expect("parallel hash: area is None");
    let new_nbuckets = {
        let pstate = pstate_mut(hashtable);
        pstate.nbuckets *= 2;
        pstate.nbuckets
    };
    let size = new_nbuckets as usize * SIZEOF_DSA_POINTER_ATOMIC;
    let batch0_dp = hashtable.batches[0].shared;
    {
        let batch0 = batch_shared_mut(hashtable, batch0_dp);
        batch0.size += size / 2;
        dsa::dsa_free::call(area, batch0.buckets);
        batch0.buckets = dsa::dsa_allocate::call(area, size);
    }
    let new_buckets_dp = batch_shared_mut(hashtable, batch0_dp).buckets;
    let buckets_addr = dsa_get_addr(area, new_buckets_dp);
    for i in 0..new_nbuckets {
        dsa_pointer_atomic_init(buckets_addr, i, INVALID_DSA_POINTER);
    }
    // Put the chunk list onto the work queue.
    let chunks = batch_shared_mut(hashtable, batch0_dp).chunks;
    {
        let pstate = pstate_mut(hashtable);
        pstate.chunk_work_queue = chunks;
        pstate.growth = ParallelHashGrowth::PHJ_GROWTH_OK;
    }
    Ok(())
}

// ===========================================================================
//   ExecParallelHashRepartitionFirst (nodeHash.c:1430)
// ===========================================================================

/// `ExecParallelHashRepartitionFirst(HashJoinTable hashtable)` — repartition
/// this backend's chunks from the work queue into the new batches.
pub fn ExecParallelHashRepartitionFirst<'mcx>(
    mcx: Mcx<'mcx>,
    hashtable: &mut HashJoinTableData<'mcx>,
) -> PgResult<()> {
    debug_assert_eq!(hashtable.nbatch, pstate_mut(hashtable).nbatch);

    while let Some((_chunk_idx, chunk_shared)) = ExecParallelHashPopChunkQueue(hashtable) {
        let area = hashtable.area.expect("parallel hash: area is None");
        let chunk_addr = dsa_get_addr(area, chunk_shared);
        let chunk_used = chunk_at(hashtable, chunk_shared).used;
        let mut idx: usize = 0;
        while idx < chunk_used {
            let hashtuple_addr = hash_chunk_data_addr(chunk_addr) + idx;
            let mintuple_addr = hjtuple_mintuple_addr(hashtuple_addr);
            let hashvalue = unsafe { (*(hashtuple_addr as *const HashJoinTupleRaw)).hashvalue };
            let t_len = mintuple_t_len(mintuple_addr);

            let bb = exec_hash_get_bucket_and_batch(hashtable, hashvalue);
            debug_assert!(bb.batchno < hashtable.nbatch);
            if bb.batchno == 0 {
                // Still belongs in batch 0. Copy to a new chunk.
                let (copy_idx, shared) =
                    ExecParallelHashTupleAlloc(mcx, hashtable, HJTUPLE_OVERHEAD + t_len)?;
                let copy_addr = copy_idx.0;
                unsafe {
                    (*(copy_addr as *mut HashJoinTupleRaw)).hashvalue = hashvalue;
                    core::ptr::copy_nonoverlapping(
                        mintuple_addr as *const u8,
                        hjtuple_mintuple_addr(copy_addr) as *mut u8,
                        t_len,
                    );
                }
                ExecParallelHashPushTuple(hashtable, bb.bucketno, copy_idx, shared);
            } else {
                // It belongs in a later batch.
                let tuple_size = MAXALIGN(HJTUPLE_OVERHEAD + t_len);
                hashtable.batches[bb.batchno as usize].estimated_size += tuple_size;
                let inner = accessor_handle(&hashtable.batches[bb.batchno as usize].inner_tuples)
                    .expect("repartition: inner_tuples accessor missing");
                sts_puttuple_raw(inner, hashvalue, mintuple_addr, t_len)?;
            }

            // Count this tuple.
            hashtable.batches[0].old_ntuples += 1;
            hashtable.batches[bb.batchno as usize].ntuples += 1;

            idx += MAXALIGN(HJTUPLE_OVERHEAD + mintuple_t_len(mintuple_addr));
        }

        // Free this chunk.
        dsa::dsa_free::call(area, chunk_shared);
        // CHECK_FOR_INTERRUPTS();
    }
    Ok(())
}

// ===========================================================================
//   ExecParallelHashRepartitionRest (nodeHash.c:1497)
// ===========================================================================

/// `ExecParallelHashRepartitionRest(HashJoinTable hashtable)` — help
/// repartition the remaining old-generation inner batches (1..old_nbatch).
pub fn ExecParallelHashRepartitionRest<'mcx>(
    mcx: Mcx<'mcx>,
    hashtable: &mut HashJoinTableData<'mcx>,
) -> PgResult<()> {
    let area = hashtable.area.expect("parallel hash: area is None");
    let (old_batches_dp, old_nbatch, nparticipants) = {
        let pstate = pstate_mut(hashtable);
        (pstate.old_batches, pstate.old_nbatch, pstate.nparticipants)
    };
    let old_batches_addr = dsa_get_addr(area, old_batches_dp);
    let pworker = backend_access_transam_parallel_seams::parallel_worker_number::call();

    // old_inner_tuples = palloc0_array(SharedTuplestoreAccessor *, old_nbatch);
    let mut old_inner_tuples: alloc_vec::Vec<Option<types_execparallel::SharedTuplestoreAccessorHandle>> =
        alloc_vec::Vec::new();
    old_inner_tuples.resize(old_nbatch as usize, None);

    for i in 1..old_nbatch {
        let shared_addr = nth_batch_addr(old_batches_addr, i, nparticipants);
        let fileset = pstate_fileset_handle(hashtable);
        let accessor = sts::sts_attach::call(
            parallel_hash_join_batch_inner(shared_addr),
            pworker + 1,
            fileset,
        )?;
        old_inner_tuples[i as usize] = Some(accessor);
    }

    for i in 1..old_nbatch {
        let accessor = old_inner_tuples[i as usize].unwrap();
        sts::sts_begin_parallel_scan_handle::call(accessor)?;
        let mut meta = [0u8; 4];
        while let Some(tuple) = sts::sts_parallel_scan_next_handle::call(mcx, accessor, &mut meta)? {
            let hashvalue = u32::from_ne_bytes(meta);
            let t_len = tuple.t_len as usize;
            let tuple_size = MAXALIGN(HJTUPLE_OVERHEAD + t_len);

            let bb = exec_hash_get_bucket_and_batch(hashtable, hashvalue);
            hashtable.batches[bb.batchno as usize].estimated_size += tuple_size;
            hashtable.batches[bb.batchno as usize].ntuples += 1;
            hashtable.batches[i as usize].old_ntuples += 1;

            let inner = accessor_handle(&hashtable.batches[bb.batchno as usize].inner_tuples)
                .expect("repartition rest: inner_tuples accessor missing");
            sts::sts_puttuple_handle::call(inner, &meta, &tuple)?;
            // CHECK_FOR_INTERRUPTS();
        }
        sts::sts_end_parallel_scan_handle::call(accessor);
    }
    Ok(())
}

// ===========================================================================
//   ExecParallelHashMergeCounters (nodeHash.c:1557)
// ===========================================================================

/// `ExecParallelHashMergeCounters(HashJoinTable hashtable)` — fold this
/// backend's partial per-batch counters into the shared totals under
/// `pstate->lock`.
pub fn ExecParallelHashMergeCounters<'mcx>(hashtable: &mut HashJoinTableData<'mcx>) {
    let my_proc_number = current_proc_number();
    let pstate = pstate_mut(hashtable);
    let guard = lwlock::lwlock_acquire::call(&pstate.lock, LWLockMode::LW_EXCLUSIVE, my_proc_number)
        .expect("ExecParallelHashMergeCounters: lwlock_acquire");
    pstate.total_tuples = 0;
    let nbatch = hashtable.nbatch;
    for i in 0..nbatch {
        let (b_size, b_estimated, b_ntuples, b_old) = {
            let acc = &hashtable.batches[i as usize];
            (acc.size, acc.estimated_size, acc.ntuples, acc.old_ntuples)
        };
        let shared_dp = hashtable.batches[i as usize].shared;
        let new_ntuples = {
            let shared = batch_shared_mut(hashtable, shared_dp);
            shared.size += b_size;
            shared.estimated_size += b_estimated;
            shared.ntuples += b_ntuples;
            shared.old_ntuples += b_old;
            shared.ntuples
        };
        {
            let acc = &mut hashtable.batches[i as usize];
            acc.size = 0;
            acc.estimated_size = 0;
            acc.ntuples = 0;
            acc.old_ntuples = 0;
        }
        pstate.total_tuples += new_ntuples;
    }
    let _ = guard.release();
}

// ===========================================================================
//   ExecParallelHashTableInsert / InsertCurrentBatch (nodeHash.c:1839 / 1905)
// ===========================================================================

/// `ExecParallelHashTableInsert(HashJoinTable hashtable, TupleTableSlot *slot,
/// uint32 hashvalue)` — insert a tuple into the shared hash table (batch 0) or
/// spill it to the appropriate shared-tuplestore batch.
pub fn ExecParallelHashTableInsert<'mcx>(
    mcx: Mcx<'mcx>,
    hashtable: &mut HashJoinTableData<'mcx>,
    slot: &types_nodes::TupleTableSlot,
    hashvalue: uint32,
) -> PgResult<()> {
    // bool shouldFree;
    // MinimalTuple tuple = ExecFetchSlotMinimalTuple(slot, &shouldFree);
    // The execTuples seam copies the slot's tuple into mcx; the owned model
    // never frees explicitly (the copy is dropped with the context). We stage
    // its flat byte image so the DSA copy below sees the real tuple bytes.
    let tuple =
        backend_executor_execTuples_seams::exec_fetch_slot_minimal_tuple_copy::call(mcx, slot)?;
    let image = tuple.to_minimal_bytes();
    let mintuple_addr = image.as_ptr() as usize;
    let t_len = tuple.t_len as usize;

    loop {
        let bb = exec_hash_get_bucket_and_batch(hashtable, hashvalue);
        if bb.batchno == 0 {
            debug_assert_eq!(
                barrier::BarrierPhase::call(&pstate_mut(hashtable).build_barrier),
                PHJ_BUILD_HASH_INNER
            );
            let (hashtuple_idx, shared) =
                match ExecParallelHashTupleAllocRetryable(mcx, hashtable, HJTUPLE_OVERHEAD + t_len)? {
                    Some(v) => v,
                    None => continue, // goto retry
                };
            let hashtuple_addr = hashtuple_idx.0;
            unsafe {
                (*(hashtuple_addr as *mut HashJoinTupleRaw)).hashvalue = hashvalue;
                core::ptr::copy_nonoverlapping(
                    mintuple_addr as *const u8,
                    hjtuple_mintuple_addr(hashtuple_addr) as *mut u8,
                    t_len,
                );
            }
            heap_tuple_header_clear_match(hjtuple_mintuple_addr(hashtuple_addr));
            ExecParallelHashPushTuple(hashtable, bb.bucketno, hashtuple_idx, shared);
        } else {
            let tuple_size = MAXALIGN(HJTUPLE_OVERHEAD + t_len);
            debug_assert!(bb.batchno > 0);
            if hashtable.batches[bb.batchno as usize].preallocated < tuple_size {
                if !ExecParallelHashTuplePrealloc(mcx, hashtable, bb.batchno, tuple_size)? {
                    continue; // goto retry
                }
            }
            debug_assert!(hashtable.batches[bb.batchno as usize].preallocated >= tuple_size);
            hashtable.batches[bb.batchno as usize].preallocated -= tuple_size;
            let inner = accessor_handle(&hashtable.batches[bb.batchno as usize].inner_tuples)
                .expect("ParallelHashTableInsert: inner_tuples missing");
            sts_puttuple_raw(inner, hashvalue, mintuple_addr, t_len)?;
        }
        hashtable.batches[bb.batchno as usize].ntuples += 1;
        break;
    }

    Ok(())
}

/// `ExecParallelHashTableInsertCurrentBatch(...)` — insert a tuple known to
/// belong in the current batch (growth disabled, no spill).
pub fn ExecParallelHashTableInsertCurrentBatch<'mcx>(
    mcx: Mcx<'mcx>,
    hashtable: &mut HashJoinTableData<'mcx>,
    slot: &types_nodes::TupleTableSlot,
    hashvalue: uint32,
) -> PgResult<()> {
    let tuple =
        backend_executor_execTuples_seams::exec_fetch_slot_minimal_tuple_copy::call(mcx, slot)?;
    let image = tuple.to_minimal_bytes();
    let mintuple_addr = image.as_ptr() as usize;
    let t_len = tuple.t_len as usize;

    let bb = exec_hash_get_bucket_and_batch(hashtable, hashvalue);
    debug_assert_eq!(bb.batchno, hashtable.curbatch);
    let (hashtuple_idx, shared) =
        ExecParallelHashTupleAlloc(mcx, hashtable, HJTUPLE_OVERHEAD + t_len)?;
    let hashtuple_addr = hashtuple_idx.0;
    unsafe {
        (*(hashtuple_addr as *mut HashJoinTupleRaw)).hashvalue = hashvalue;
        core::ptr::copy_nonoverlapping(
            mintuple_addr as *const u8,
            hjtuple_mintuple_addr(hashtuple_addr) as *mut u8,
            t_len,
        );
    }
    heap_tuple_header_clear_match(hjtuple_mintuple_addr(hashtuple_addr));
    ExecParallelHashPushTuple(hashtable, bb.bucketno, hashtuple_idx, shared);

    Ok(())
}

// ===========================================================================
//   ExecParallelScanHashBucket / Prep / ScanForUnmatched (nodeHash.c:2053..)
// ===========================================================================

/// `ExecParallelScanHashBucket(HashJoinState *hjstate, ExprContext *econtext)`
/// — scan a shared hash bucket for matches to the current outer tuple.
pub fn ExecParallelScanHashBucket<'mcx>(
    hjstate: &mut HashJoinState<'mcx>,
    estate: &mut types_nodes::EStateData<'mcx>,
) -> PgResult<bool> {
    let mcx = estate.es_query_cxt;
    let hashvalue = hjstate.hj_CurHashValue;
    let cur_bucket = hjstate.hj_CurBucketNo;
    let hash_tuple_slot = hjstate
        .hj_HashTupleSlot
        .expect("ExecParallelScanHashBucket: hj_HashTupleSlot must be set up by init");
    let econtext_id = hjstate
        .js
        .ps
        .ps_ExprContext
        .expect("ExecParallelScanHashBucket: ps_ExprContext");
    let hashclauses = hjstate
        .hashclauses
        .as_deref()
        .expect("ExecParallelScanHashBucket: hashclauses must be compiled by init")
        as *const types_nodes::execexpr::ExprState;

    let hashtable = hjstate
        .hj_HashTable
        .as_ref()
        .expect("ExecParallelScanHashBucket: hj_HashTable is None")
        .as_ref();

    let mut hash_tuple = match hjstate.hj_CurTuple {
        Some(t) => ExecParallelHashNextTuple(hashtable, t),
        None => ExecParallelHashFirstTuple(hashtable, cur_bucket),
    };

    while let Some(ht) = hash_tuple {
        let ht_hashvalue = unsafe { (*(ht.0 as *const HashJoinTupleRaw)).hashvalue };
        if ht_hashvalue == hashvalue {
            // insert hashtable's tuple into exec slot so ExecQual sees it:
            //   inntuple = ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple),
            //                                    hj_HashTupleSlot, false);
            //   econtext->ecxt_innertuple = inntuple;
            // The shared tuple is a flat MinimalTuple byte image in DSA;
            // reconstruct it into mcx and force-store into the hash-tuple slot.
            let mtup = mintuple_from_dsa(mcx, hjtuple_mintuple_addr(ht.0))?;
            let mtup = mcx::alloc_in(mcx, mtup)?;
            backend_executor_execTuples_seams::exec_force_store_minimal_tuple::call(
                hash_tuple_slot,
                mtup,
                false,
                estate,
            )?;
            estate.ecxt_mut(econtext_id).ecxt_innertuple = Some(hash_tuple_slot);
            // if (ExecQualAndReset(hjclauses, econtext)) { hj_CurTuple = ht; return true; }
            let pass = backend_executor_execExpr_seams::exec_qual_and_reset::call(
                unsafe { &*hashclauses },
                econtext_id,
                estate,
            )?;
            if pass {
                hjstate.hj_CurTuple = Some(ht);
                return Ok(true);
            }
        }
        let hashtable = hjstate
            .hj_HashTable
            .as_ref()
            .expect("ExecParallelScanHashBucket: hj_HashTable is None")
            .as_ref();
        hash_tuple = ExecParallelHashNextTuple(hashtable, ht);
    }
    Ok(false)
}

/// `ExecParallelPrepHashTableForUnmatched(HashJoinState *hjstate)` — wait-free
/// election to scan unmatched inner tuples; `true` if this backend won.
pub fn ExecParallelPrepHashTableForUnmatched<'mcx>(
    hjstate: &mut HashJoinState<'mcx>,
) -> PgResult<bool> {
    let curbatch = hjstate
        .hj_HashTable
        .as_ref()
        .expect("ExecParallelPrepHashTableForUnmatched: hj_HashTable")
        .curbatch;
    let batch_dp = {
        let hashtable = hjstate.hj_HashTable.as_ref().unwrap();
        hashtable.batches[curbatch as usize].shared
    };

    // Assert(BarrierPhase(&batch->batch_barrier) == PHJ_BATCH_PROBE);
    {
        let hashtable = hjstate.hj_HashTable.as_ref().unwrap();
        let batch = batch_shared_mut(hashtable, batch_dp);
        debug_assert_eq!(
            barrier::BarrierPhase::call(&batch.batch_barrier),
            types_nodes::nodehash::PHJ_BATCH_PROBE
        );
    }

    let won_election = {
        let hashtable = hjstate.hj_HashTable.as_ref().unwrap();
        let batch = batch_shared_mut(hashtable, batch_dp);
        barrier::BarrierArriveAndDetachExceptLast::call(&mut batch.batch_barrier)
    };

    if !won_election {
        // This process considers the batch to be done.
        {
            let hashtable = hjstate.hj_HashTable.as_mut().unwrap().as_mut();
            hashtable.batches[curbatch as usize].done = true;
            let inner = accessor_handle(&hashtable.batches[curbatch as usize].inner_tuples);
            let outer = accessor_handle(&hashtable.batches[curbatch as usize].outer_tuples);
            if let Some(a) = inner {
                sts::sts_end_parallel_scan_handle::call(a);
            }
            if let Some(a) = outer {
                sts::sts_end_parallel_scan_handle::call(a);
            }
        }
        // Track largest batch.
        let hashtable = hjstate.hj_HashTable.as_mut().unwrap().as_mut();
        let nbuckets = hashtable.nbuckets;
        let batch_size = batch_shared_mut(hashtable, batch_dp).size;
        hashtable.spacePeak = hashtable
            .spacePeak
            .max(batch_size + SIZEOF_DSA_POINTER_ATOMIC * nbuckets as usize);
        hashtable.curbatch = -1;
        return Ok(false);
    }

    // Now alone with this batch.
    {
        let hashtable = hjstate.hj_HashTable.as_ref().unwrap();
        let batch = batch_shared_mut(hashtable, batch_dp);
        debug_assert_eq!(
            barrier::BarrierPhase::call(&batch.batch_barrier),
            types_nodes::nodehash::PHJ_BATCH_SCAN
        );
    }

    let skip_unmatched = {
        let hashtable = hjstate.hj_HashTable.as_ref().unwrap();
        batch_shared_mut(hashtable, batch_dp).skip_unmatched
    };
    if skip_unmatched {
        {
            let hashtable = hjstate.hj_HashTable.as_mut().unwrap().as_mut();
            hashtable.batches[curbatch as usize].done = true;
            ExecHashTableDetachBatch(hashtable)?;
        }
        return Ok(false);
    }

    // Prepare process-local state, just as for non-parallel join.
    crate::hash_table::ExecPrepHashTableForUnmatched(hjstate);
    Ok(true)
}

/// `ExecParallelScanHashTableForUnmatched(HashJoinState *hjstate,
/// ExprContext *econtext)` — return the next unmatched inner tuple.
pub fn ExecParallelScanHashTableForUnmatched<'mcx>(
    hjstate: &mut HashJoinState<'mcx>,
    estate: &mut types_nodes::EStateData<'mcx>,
) -> PgResult<bool> {
    let mcx = estate.es_query_cxt;
    let hash_tuple_slot = hjstate
        .hj_HashTupleSlot
        .expect("ExecParallelScanHashTableForUnmatched: hj_HashTupleSlot must be set up by init");
    let econtext_id = hjstate
        .js
        .ps
        .ps_ExprContext
        .expect("ExecParallelScanHashTableForUnmatched: ps_ExprContext");
    let mut hash_tuple = hjstate.hj_CurTuple;

    loop {
        // pick the next tuple / next bucket
        if let Some(ht) = hash_tuple {
            let hashtable = hjstate.hj_HashTable.as_ref().unwrap().as_ref();
            hash_tuple = ExecParallelHashNextTuple(hashtable, ht);
        } else {
            let nbuckets = hjstate.hj_HashTable.as_ref().unwrap().nbuckets;
            if hjstate.hj_CurBucketNo < nbuckets {
                let bucketno = hjstate.hj_CurBucketNo;
                hjstate.hj_CurBucketNo += 1;
                let hashtable = hjstate.hj_HashTable.as_ref().unwrap().as_ref();
                hash_tuple = ExecParallelHashFirstTuple(hashtable, bucketno);
            } else {
                break; // finished all buckets
            }
        }

        while let Some(ht) = hash_tuple {
            let mintuple_addr = hjtuple_mintuple_addr(ht.0);
            if !heap_tuple_header_has_match(mintuple_addr) {
                // insert hashtable's tuple into exec slot:
                //   inntuple = ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple),
                //                                    hj_HashTupleSlot, false);
                //   econtext->ecxt_innertuple = inntuple;  ResetExprContext(econtext);
                let mtup = mintuple_from_dsa(mcx, mintuple_addr)?;
                let mtup = mcx::alloc_in(mcx, mtup)?;
                backend_executor_execTuples_seams::exec_force_store_minimal_tuple::call(
                    hash_tuple_slot,
                    mtup,
                    false,
                    estate,
                )?;
                estate.ecxt_mut(econtext_id).ecxt_innertuple = Some(hash_tuple_slot);
                backend_executor_execUtils_seams::reset_per_tuple_expr_context::call(
                    estate,
                    &hjstate.js.ps,
                )?;
                hjstate.hj_CurTuple = Some(ht);
                return Ok(true);
            }
            let hashtable = hjstate.hj_HashTable.as_ref().unwrap().as_ref();
            hash_tuple = ExecParallelHashNextTuple(hashtable, ht);
        }
        // CHECK_FOR_INTERRUPTS();
    }
    Ok(false)
}

// ===========================================================================
//   ExecParallelHashTupleAlloc (nodeHash.c:2976)
// ===========================================================================

/// `ExecParallelHashTupleAlloc(HashJoinTable hashtable, size_t size,
/// dsa_pointer *shared)` — allocate dense tuple space in DSA. Returns the
/// tuple's address-as-index and its shared `dsa_pointer`. The retryable
/// variant returns `None` when the caller must retry (a grow was triggered).
pub fn ExecParallelHashTupleAlloc<'mcx>(
    mcx: Mcx<'mcx>,
    hashtable: &mut HashJoinTableData<'mcx>,
    size: Size,
) -> PgResult<(HashTupleIdx, DsaPointer)> {
    // The non-retryable wrapper used by InsertCurrentBatch / RepartitionFirst,
    // where C never retries (growth disabled / batch-0 copy).
    loop {
        if let Some(v) = ExecParallelHashTupleAllocRetryable(mcx, hashtable, size)? {
            return Ok(v);
        }
    }
}

/// The retry-aware form: `Ok(None)` is the C `return NULL` "caller must retry".
fn ExecParallelHashTupleAllocRetryable<'mcx>(
    mcx: Mcx<'mcx>,
    hashtable: &mut HashJoinTableData<'mcx>,
    size: Size,
) -> PgResult<Option<(HashTupleIdx, DsaPointer)>> {
    let area = hashtable.area.expect("parallel hash: area is None");
    let size = MAXALIGN(size);
    let curbatch = hashtable.curbatch;

    // Fast path: enough space in this backend's current chunk.
    let chunk_shared = hashtable.current_chunk_shared;
    if hashtable.current_chunk.is_some() && size <= HASH_CHUNK_THRESHOLD {
        let chunk = chunk_at(hashtable, chunk_shared);
        if chunk.maxlen - chunk.used >= size {
            let shared = chunk_shared + HASH_CHUNK_HEADER_SIZE as u64 + chunk.used as u64;
            let chunk_addr = dsa_get_addr(area, chunk_shared);
            let result = hash_chunk_data_addr(chunk_addr) + chunk.used;
            chunk.used += size;
            debug_assert!(chunk.used <= chunk.maxlen);
            return Ok(Some((HashTupleIdx(result), shared)));
        }
    }

    // Slow path: allocate a new chunk under pstate->lock.
    let my_proc_number = current_proc_number();
    let pstate = pstate_mut(hashtable);
    let guard = lwlock::lwlock_acquire::call(&pstate.lock, LWLockMode::LW_EXCLUSIVE, my_proc_number)?;

    // Help increase the number of buckets or batches?
    let growth = pstate.growth;
    if growth == ParallelHashGrowth::PHJ_GROWTH_NEED_MORE_BATCHES
        || growth == ParallelHashGrowth::PHJ_GROWTH_NEED_MORE_BUCKETS
    {
        hashtable.current_chunk = None;
        let _ = guard.release();
        if growth == ParallelHashGrowth::PHJ_GROWTH_NEED_MORE_BATCHES {
            ExecParallelHashIncreaseNumBatches(mcx, hashtable)?;
        } else {
            ExecParallelHashIncreaseNumBuckets(mcx, hashtable)?;
        }
        return Ok(None); // caller must retry
    }

    // Oversized tuples get their own chunk.
    let chunk_size = if size > HASH_CHUNK_THRESHOLD {
        size + HASH_CHUNK_HEADER_SIZE
    } else {
        HASH_CHUNK_SIZE
    };

    // Check if it's time to grow batches or buckets.
    if pstate.growth != ParallelHashGrowth::PHJ_GROWTH_DISABLED {
        debug_assert_eq!(curbatch, 0);
        debug_assert_eq!(
            barrier::BarrierPhase::call(&pstate.build_barrier),
            PHJ_BUILD_HASH_INNER
        );

        let batch0_dp = hashtable.batches[0].shared;
        // Space limit check.
        let at_least_one = hashtable.batches[0].at_least_one_chunk;
        let shared_size = batch_shared_mut(hashtable, batch0_dp).size;
        if at_least_one && shared_size + chunk_size > pstate.space_allowed {
            pstate.growth = ParallelHashGrowth::PHJ_GROWTH_NEED_MORE_BATCHES;
            batch_shared_mut(hashtable, batch0_dp).space_exhausted = true;
            let _ = guard.release();
            return Ok(None);
        }

        // Load factor check.
        if hashtable.nbatch == 1 {
            let local_ntuples = hashtable.batches[0].ntuples;
            let new_shared_ntuples = {
                let shared = batch_shared_mut(hashtable, batch0_dp);
                shared.ntuples += local_ntuples;
                shared.ntuples
            };
            hashtable.batches[0].ntuples = 0;
            if new_shared_ntuples + 1 > (hashtable.nbuckets * NTUP_PER_BUCKET) as usize
                && hashtable.nbuckets < INT_MAX / 2
                && (hashtable.nbuckets * 2) as usize <= MaxAllocSize / SIZEOF_DSA_POINTER_ATOMIC
            {
                pstate.growth = ParallelHashGrowth::PHJ_GROWTH_NEED_MORE_BUCKETS;
                let _ = guard.release();
                return Ok(None);
            }
        }
    }

    // Cleared to allocate a new chunk.
    let chunk_shared_new = dsa::dsa_allocate::call(area, chunk_size);
    let curbatch_dp = hashtable.batches[curbatch as usize].shared;
    batch_shared_mut(hashtable, curbatch_dp).size += chunk_size;
    hashtable.batches[curbatch as usize].at_least_one_chunk = true;

    // Set up the chunk.
    let chunk_addr = dsa_get_addr(area, chunk_shared_new);
    let shared = chunk_shared_new + HASH_CHUNK_HEADER_SIZE as u64;
    let prev_chunks = batch_shared_mut(hashtable, curbatch_dp).chunks;
    {
        let chunk = unsafe { &mut *(chunk_addr as *mut HashMemoryChunkRaw) };
        chunk.maxlen = chunk_size - HASH_CHUNK_HEADER_SIZE;
        chunk.used = size;
        chunk.ntuples = 0;
        chunk.next_shared = prev_chunks;
    }
    batch_shared_mut(hashtable, curbatch_dp).chunks = chunk_shared_new;

    if size <= HASH_CHUNK_THRESHOLD {
        hashtable.current_chunk = Some(HashChunkIdx(chunk_addr));
        hashtable.current_chunk_shared = chunk_shared_new;
    }
    let _ = guard.release();

    let result = hash_chunk_data_addr(chunk_addr);
    Ok(Some((HashTupleIdx(result), shared)))
}

// ===========================================================================
//   ExecParallelHashJoinSetUpBatches (nodeHash.c:3124)
// ===========================================================================

/// `ExecParallelHashJoinSetUpBatches(HashJoinTable hashtable, int nbatch)` —
/// allocate the shared `ParallelHashJoinBatch` array and per-batch tuplestores.
pub fn ExecParallelHashJoinSetUpBatches<'mcx>(
    mcx: Mcx<'mcx>,
    hashtable: &mut HashJoinTableData<'mcx>,
    nbatch: i32,
) -> PgResult<()> {
    debug_assert!(hashtable.batches.is_empty());
    let area = hashtable.area.expect("parallel hash: area is None");
    let nparticipants = pstate_mut(hashtable).nparticipants;
    let pworker = backend_access_transam_parallel_seams::parallel_worker_number::call();

    // pstate->batches = dsa_allocate0(area, Estimate * nbatch);
    let total = estimate_parallel_hash_join_batch(nparticipants) * nbatch as usize;
    let batches_dp = dsa::dsa_allocate::call(area, total);
    let batches_base = dsa_get_addr(area, batches_dp);
    // dsa_allocate0: zero the region (C uses dsa_allocate0).
    unsafe {
        core::ptr::write_bytes(batches_base as *mut u8, 0, total);
    }
    {
        let pstate = pstate_mut(hashtable);
        pstate.batches = batches_dp;
        pstate.nbatch = nbatch;
    }

    // Allocate this backend's accessor array (in mcx, the spill context).
    hashtable.nbatch = nbatch;
    let mut accessors = mcx::vec_with_capacity_in(mcx, nbatch as usize)?;
    for i in 0..nbatch {
        let shared_addr = nth_batch_addr(batches_base, i, nparticipants);
        let shared_dp = batches_dp + (estimate_parallel_hash_join_batch(nparticipants) * i as usize) as u64;

        // Set up the Barrier (all other members are zeroed).
        {
            let shared = batch_at_addr::<'_>(shared_addr);
            barrier::BarrierInit::call(&mut shared.batch_barrier, 0);
            if i == 0 {
                barrier::BarrierAttach::call(&mut shared.batch_barrier);
                while barrier::BarrierPhase::call(&shared.batch_barrier)
                    < types_nodes::nodehash::PHJ_BATCH_PROBE
                {
                    barrier::BarrierArriveAndWait::call(&mut shared.batch_barrier, 0);
                }
                barrier::BarrierDetach::call(&mut shared.batch_barrier);
            }
        }

        // Initialize the shared tuplestores.
        let fileset = pstate_fileset_handle(hashtable);
        let inner_name = alloc_format::format(format_args!("i{}of{}", i, nbatch));
        let inner = sts::sts_initialize::call(
            parallel_hash_join_batch_inner(shared_addr),
            nparticipants,
            pworker + 1,
            core::mem::size_of::<uint32>(),
            SHARED_TUPLESTORE_SINGLE_PASS,
            fileset,
            &inner_name,
        )?;
        let outer_name = alloc_format::format(format_args!("o{}of{}", i, nbatch));
        let outer = sts::sts_initialize::call(
            parallel_hash_join_batch_outer(shared_addr, nparticipants),
            nparticipants,
            pworker + 1,
            core::mem::size_of::<uint32>(),
            SHARED_TUPLESTORE_SINGLE_PASS,
            fileset,
            &outer_name,
        )?;

        accessors.push(new_accessor(mcx, shared_dp, Some(inner), Some(outer))?);
    }
    hashtable.batches = accessors;
    Ok(())
}

// ===========================================================================
//   ExecParallelHashCloseBatchAccessors (nodeHash.c:3204)
// ===========================================================================

/// `ExecParallelHashCloseBatchAccessors(HashJoinTable hashtable)` — detach this
/// backend's per-batch tuplestore accessors and free the accessor array.
pub fn ExecParallelHashCloseBatchAccessors<'mcx>(
    hashtable: &mut HashJoinTableData<'mcx>,
) -> PgResult<()> {
    let nbatch = hashtable.nbatch;
    for i in 0..nbatch {
        let inner = accessor_handle(&hashtable.batches[i as usize].inner_tuples);
        let outer = accessor_handle(&hashtable.batches[i as usize].outer_tuples);
        if let Some(a) = inner {
            sts::sts_end_write_handle::call(a)?;
        }
        if let Some(a) = outer {
            sts::sts_end_write_handle::call(a)?;
        }
        if let Some(a) = inner {
            sts::sts_end_parallel_scan_handle::call(a);
        }
        if let Some(a) = outer {
            sts::sts_end_parallel_scan_handle::call(a);
        }
    }
    hashtable.batches.clear();
    Ok(())
}

// ===========================================================================
//   ExecParallelHashEnsureBatchAccessors (nodeHash.c:3225)
// ===========================================================================

/// `ExecParallelHashEnsureBatchAccessors(HashJoinTable hashtable)` — lazily
/// (re)create this backend's per-batch accessors, attaching to the shared
/// batches' tuplestores. Allocates in `mcx`.
pub fn ExecParallelHashEnsureBatchAccessors<'mcx>(
    mcx: Mcx<'mcx>,
    hashtable: &mut HashJoinTableData<'mcx>,
) -> PgResult<()> {
    let pstate_nbatch = pstate_mut(hashtable).nbatch;
    if !hashtable.batches.is_empty() {
        if hashtable.nbatch == pstate_nbatch {
            return Ok(());
        }
        ExecParallelHashCloseBatchAccessors(hashtable)?;
    }

    debug_assert!(dsa_pointer_is_valid(pstate_mut(hashtable).batches));

    let area = hashtable.area.expect("parallel hash: area is None");
    let (batches_dp, nparticipants) = {
        let pstate = pstate_mut(hashtable);
        (pstate.batches, pstate.nparticipants)
    };
    let pworker = backend_access_transam_parallel_seams::parallel_worker_number::call();

    hashtable.nbatch = pstate_nbatch;
    let batches_base = dsa_get_addr(area, batches_dp);

    let mut accessors = mcx::vec_with_capacity_in(mcx, pstate_nbatch as usize)?;
    for i in 0..pstate_nbatch {
        let shared_addr = nth_batch_addr(batches_base, i, nparticipants);
        let shared_dp =
            batches_dp + (estimate_parallel_hash_join_batch(nparticipants) * i as usize) as u64;
        let fileset = pstate_fileset_handle(hashtable);
        let inner = sts::sts_attach::call(
            parallel_hash_join_batch_inner(shared_addr),
            pworker + 1,
            fileset,
        )?;
        let outer = sts::sts_attach::call(
            parallel_hash_join_batch_outer(shared_addr, nparticipants),
            pworker + 1,
            fileset,
        )?;
        accessors.push(new_accessor(mcx, shared_dp, Some(inner), Some(outer))?);
    }
    hashtable.batches = accessors;
    Ok(())
}

// ===========================================================================
//   ExecParallelHashTableAlloc (nodeHash.c:3289)
// ===========================================================================

/// `ExecParallelHashTableAlloc(HashJoinTable hashtable, int batchno)` —
/// allocate the shared bucket array for one batch in DSA, initializing each
/// `dsa_pointer_atomic` to `InvalidDsaPointer`.
pub fn ExecParallelHashTableAlloc<'mcx>(
    _mcx: Mcx<'mcx>,
    hashtable: &mut HashJoinTableData<'mcx>,
    batchno: i32,
) -> PgResult<()> {
    let area = hashtable.area.expect("parallel hash: area is None");
    let nbuckets = pstate_mut(hashtable).nbuckets;
    let batch_dp = hashtable.batches[batchno as usize].shared;

    let buckets = dsa::dsa_allocate::call(area, SIZEOF_DSA_POINTER_ATOMIC * nbuckets as usize);
    batch_shared_mut(hashtable, batch_dp).buckets = buckets;
    let buckets_addr = dsa_get_addr(area, buckets);
    for i in 0..nbuckets {
        dsa_pointer_atomic_init(buckets_addr, i, INVALID_DSA_POINTER);
    }
    Ok(())
}

// ===========================================================================
//   ExecHashTableDetachBatch (nodeHash.c:3309)
// ===========================================================================

/// `ExecHashTableDetachBatch(HashJoinTable hashtable)` — detach from the
/// current batch's barrier, freeing its shared memory if last out.
pub fn ExecHashTableDetachBatch<'mcx>(hashtable: &mut HashJoinTableData<'mcx>) -> PgResult<()> {
    if hashtable.parallel_state.is_some() && hashtable.curbatch >= 0 {
        let curbatch = hashtable.curbatch;
        let area = hashtable.area.expect("parallel hash: area is None");
        let batch_dp = hashtable.batches[curbatch as usize].shared;

        // Make sure any temporary files are closed.
        let inner = accessor_handle(&hashtable.batches[curbatch as usize].inner_tuples);
        let outer = accessor_handle(&hashtable.batches[curbatch as usize].outer_tuples);
        if let Some(a) = inner {
            sts::sts_end_parallel_scan_handle::call(a);
        }
        if let Some(a) = outer {
            sts::sts_end_parallel_scan_handle::call(a);
        }

        let mut attached = true;
        let phase = {
            let batch = batch_shared_mut(hashtable, batch_dp);
            barrier::BarrierPhase::call(&batch.batch_barrier)
        };
        debug_assert!(
            phase == types_nodes::nodehash::PHJ_BATCH_PROBE
                || phase == types_nodes::nodehash::PHJ_BATCH_SCAN
        );

        if phase == types_nodes::nodehash::PHJ_BATCH_PROBE
            && !hashtable.batches[curbatch as usize].outer_eof
        {
            batch_shared_mut(hashtable, batch_dp).skip_unmatched = true;
        }

        if phase == types_nodes::nodehash::PHJ_BATCH_PROBE {
            let batch = batch_shared_mut(hashtable, batch_dp);
            attached = barrier::BarrierArriveAndDetachExceptLast::call(&mut batch.batch_barrier);
        }
        let last = {
            let batch = batch_shared_mut(hashtable, batch_dp);
            attached && barrier::BarrierArriveAndDetach::call(&mut batch.batch_barrier)
        };
        if last {
            debug_assert_eq!(
                barrier::BarrierPhase::call(&batch_shared_mut(hashtable, batch_dp).batch_barrier),
                types_nodes::nodehash::PHJ_BATCH_FREE
            );
            // Free shared chunks and buckets.
            loop {
                let chunks = batch_shared_mut(hashtable, batch_dp).chunks;
                if !dsa_pointer_is_valid(chunks) {
                    break;
                }
                let next = chunk_at(hashtable, chunks).next_shared;
                dsa::dsa_free::call(area, chunks);
                batch_shared_mut(hashtable, batch_dp).chunks = next;
            }
            let buckets = batch_shared_mut(hashtable, batch_dp).buckets;
            if dsa_pointer_is_valid(buckets) {
                dsa::dsa_free::call(area, buckets);
                batch_shared_mut(hashtable, batch_dp).buckets = INVALID_DSA_POINTER;
            }
        }

        // Track the largest batch we've been attached to.
        let nbuckets = hashtable.nbuckets;
        let batch_size = batch_shared_mut(hashtable, batch_dp).size;
        hashtable.spacePeak = hashtable
            .spacePeak
            .max(batch_size + SIZEOF_DSA_POINTER_ATOMIC * nbuckets as usize);

        hashtable.curbatch = -1;
    }
    Ok(())
}

// ===========================================================================
//   ExecHashTableDetach (nodeHash.c:3401)
// ===========================================================================

/// `ExecHashTableDetach(HashJoinTable hashtable)` — detach from the whole
/// parallel hash join, freeing the shared state if last out.
pub fn ExecHashTableDetach<'mcx>(hashtable: &mut HashJoinTableData<'mcx>) -> PgResult<()> {
    if hashtable.parallel_state.is_some() {
        let area = hashtable.area.expect("parallel hash: area is None");

        // Assert build_barrier >= PHJ_BUILD_RUN.
        let build_phase = barrier::BarrierPhase::call(&pstate_mut(hashtable).build_barrier);
        debug_assert!(build_phase >= types_nodes::nodehash::PHJ_BUILD_RUN);

        if build_phase == types_nodes::nodehash::PHJ_BUILD_RUN {
            // Make sure any temporary files are closed.
            if !hashtable.batches.is_empty() {
                let nbatch = hashtable.nbatch;
                for i in 0..nbatch {
                    let inner = accessor_handle(&hashtable.batches[i as usize].inner_tuples);
                    let outer = accessor_handle(&hashtable.batches[i as usize].outer_tuples);
                    if let Some(a) = inner {
                        sts::sts_end_write_handle::call(a)?;
                    }
                    if let Some(a) = outer {
                        sts::sts_end_write_handle::call(a)?;
                    }
                    if let Some(a) = inner {
                        sts::sts_end_parallel_scan_handle::call(a);
                    }
                    if let Some(a) = outer {
                        sts::sts_end_parallel_scan_handle::call(a);
                    }
                }
            }

            // If we're last to detach, clean up shared memory.
            let last = {
                let pstate = pstate_mut(hashtable);
                barrier::BarrierArriveAndDetach::call(&mut pstate.build_barrier)
            };
            if last {
                let pstate = pstate_mut(hashtable);
                debug_assert_eq!(
                    barrier::BarrierPhase::call(&pstate.build_barrier),
                    types_nodes::nodehash::PHJ_BUILD_FREE
                );
                if dsa_pointer_is_valid(pstate.batches) {
                    dsa::dsa_free::call(area, pstate.batches);
                    pstate.batches = INVALID_DSA_POINTER;
                }
            }
        }
    }
    hashtable.parallel_state = None;
    Ok(())
}

// ===========================================================================
//   inline bucket-chain primitives (nodeHash.c:3451 / 3467 / 3481)
// ===========================================================================

/// `ExecParallelHashFirstTuple(HashJoinTable hashtable, int bucketno)` — the
/// head tuple of a shared bucket (`None` = empty).
pub fn ExecParallelHashFirstTuple<'mcx>(
    hashtable: &HashJoinTableData<'mcx>,
    bucketno: i32,
) -> Option<HashTupleIdx> {
    debug_assert!(hashtable.parallel_state.is_some());
    let area = hashtable.area.expect("parallel hash: area is None");
    let buckets_addr = current_shared_buckets(hashtable);
    let p = bucket_atomic_read(buckets_addr, bucketno);
    if !dsa_pointer_is_valid(p) {
        return None;
    }
    Some(HashTupleIdx(dsa_get_addr(area, p)))
}

/// `ExecParallelHashNextTuple(HashJoinTable hashtable, HashJoinTuple tuple)` —
/// the next tuple in the same bucket chain.
pub fn ExecParallelHashNextTuple<'mcx>(
    hashtable: &HashJoinTableData<'mcx>,
    tuple: HashTupleIdx,
) -> Option<HashTupleIdx> {
    debug_assert!(hashtable.parallel_state.is_some());
    let area = hashtable.area.expect("parallel hash: area is None");
    let next = unsafe { (*(tuple.0 as *const HashJoinTupleRaw)).next_shared };
    if !dsa_pointer_is_valid(next) {
        return None;
    }
    Some(HashTupleIdx(dsa_get_addr(area, next)))
}

/// `ExecParallelHashPushTuple(dsa_pointer_atomic *head, HashJoinTuple tuple,
/// dsa_pointer tuple_shared)` — atomically push a tuple onto the head of a
/// shared bucket chain (CAS loop). `bucketno` selects the head atomic from the
/// current batch's bucket array.
pub fn ExecParallelHashPushTuple<'mcx>(
    hashtable: &mut HashJoinTableData<'mcx>,
    bucketno: i32,
    tuple: HashTupleIdx,
    tuple_shared: DsaPointer,
) {
    let buckets_addr = current_shared_buckets(hashtable);
    let head = (buckets_addr + bucketno as usize * SIZEOF_DSA_POINTER_ATOMIC) as *const AtomicU64;
    let tuple_raw = tuple.0 as *mut HashJoinTupleRaw;
    loop {
        let cur = unsafe { (*head).load(Ordering::Acquire) };
        unsafe {
            (*tuple_raw).next_shared = cur;
        }
        let res = unsafe {
            (*head).compare_exchange(cur, tuple_shared, Ordering::AcqRel, Ordering::Acquire)
        };
        if res.is_ok() {
            break;
        }
    }
}

// ===========================================================================
//   ExecParallelHashTableSetCurrentBatch (nodeHash.c:3499)
// ===========================================================================

/// `ExecParallelHashTableSetCurrentBatch(HashJoinTable hashtable, int batchno)`
/// — point the backend-local table state at one shared batch.
pub fn ExecParallelHashTableSetCurrentBatch<'mcx>(
    hashtable: &mut HashJoinTableData<'mcx>,
    batchno: i32,
) {
    let batch_dp = hashtable.batches[batchno as usize].shared;
    debug_assert!(dsa_pointer_is_valid(batch_shared_mut(hashtable, batch_dp).buckets));

    hashtable.curbatch = batchno;
    // hashtable->buckets.shared = dsa_get_address(area, batch->buckets);
    // The owned model already resolves buckets.shared from the current batch
    // accessor (see current_shared_buckets); we just record dimensions.
    let nbuckets = pstate_mut(hashtable).nbuckets;
    hashtable.nbuckets = nbuckets;
    hashtable.log2_nbuckets = my_log2(nbuckets as i64);
    hashtable.current_chunk = None;
    hashtable.current_chunk_shared = INVALID_DSA_POINTER;
    hashtable.batches[batchno as usize].at_least_one_chunk = false;
}

// ===========================================================================
//   ExecParallelHashPopChunkQueue (nodeHash.c:3520)
// ===========================================================================

/// `ExecParallelHashPopChunkQueue(HashJoinTable hashtable, dsa_pointer *shared)`
/// — atomically pop a chunk off the shared work queue under `pstate->lock`;
/// returns the chunk's address-as-index and its `dsa_pointer` (`None` = empty).
pub fn ExecParallelHashPopChunkQueue<'mcx>(
    hashtable: &mut HashJoinTableData<'mcx>,
) -> Option<(HashChunkIdx, DsaPointer)> {
    let area = hashtable.area.expect("parallel hash: area is None");
    let my_proc_number = current_proc_number();
    let pstate = pstate_mut(hashtable);
    let guard = lwlock::lwlock_acquire::call(&pstate.lock, LWLockMode::LW_EXCLUSIVE, my_proc_number)
        .expect("ExecParallelHashPopChunkQueue: lwlock_acquire");

    let result = if dsa_pointer_is_valid(pstate.chunk_work_queue) {
        let shared = pstate.chunk_work_queue;
        let chunk_addr = dsa_get_addr(area, shared);
        let next = unsafe { (*(chunk_addr as *const HashMemoryChunkRaw)).next_shared };
        pstate.chunk_work_queue = next;
        Some((HashChunkIdx(chunk_addr), shared))
    } else {
        None
    };
    let _ = guard.release();
    result
}

// ===========================================================================
//   ExecParallelHashTuplePrealloc (nodeHash.c:3561)
// ===========================================================================

/// `ExecParallelHashTuplePrealloc(HashJoinTable hashtable, int batchno,
/// size_t size)` — reserve shared space for an upcoming tuple under
/// `pstate->lock`; `false` means the dimensions changed and the caller must
/// reconsider and call again.
pub fn ExecParallelHashTuplePrealloc<'mcx>(
    mcx: Mcx<'mcx>,
    hashtable: &mut HashJoinTableData<'mcx>,
    batchno: i32,
    size: Size,
) -> PgResult<bool> {
    let want = size.max(HASH_CHUNK_SIZE - HASH_CHUNK_HEADER_SIZE);

    debug_assert!(batchno > 0);
    debug_assert!(batchno < hashtable.nbatch);
    debug_assert_eq!(size, MAXALIGN(size));

    let my_proc_number = current_proc_number();
    let pstate = pstate_mut(hashtable);
    let guard =
        lwlock::lwlock_acquire::call(&pstate.lock, LWLockMode::LW_EXCLUSIVE, my_proc_number)?;

    // Has another participant commanded us to help grow?
    let growth = pstate.growth;
    if growth == ParallelHashGrowth::PHJ_GROWTH_NEED_MORE_BATCHES
        || growth == ParallelHashGrowth::PHJ_GROWTH_NEED_MORE_BUCKETS
    {
        let _ = guard.release();
        if growth == ParallelHashGrowth::PHJ_GROWTH_NEED_MORE_BATCHES {
            ExecParallelHashIncreaseNumBatches(mcx, hashtable)?;
        } else {
            ExecParallelHashIncreaseNumBuckets(mcx, hashtable)?;
        }
        return Ok(false);
    }

    let batch_dp = hashtable.batches[batchno as usize].shared;
    let at_least_one = hashtable.batches[batchno as usize].at_least_one_chunk;
    let space_allowed = pstate.space_allowed;
    let estimated = batch_shared_mut(hashtable, batch_dp).estimated_size;
    if pstate_mut(hashtable).growth != ParallelHashGrowth::PHJ_GROWTH_DISABLED
        && at_least_one
        && estimated + want + HASH_CHUNK_HEADER_SIZE > space_allowed
    {
        batch_shared_mut(hashtable, batch_dp).space_exhausted = true;
        pstate_mut(hashtable).growth = ParallelHashGrowth::PHJ_GROWTH_NEED_MORE_BATCHES;
        let _ = guard.release();
        return Ok(false);
    }

    hashtable.batches[batchno as usize].at_least_one_chunk = true;
    batch_shared_mut(hashtable, batch_dp).estimated_size += want + HASH_CHUNK_HEADER_SIZE;
    hashtable.batches[batchno as usize].preallocated = want;
    let _ = guard.release();
    Ok(true)
}

// ===========================================================================
//   Small nodeHash-owned shared-state accessors the parallel-aware new-batch
//   loop reaches as seams (they touch the DSM-resident pstate / batch_barrier
//   / distributor). Each mirrors the matching C one-liner in nodeHashjoin.c.
// ===========================================================================

#[inline]
fn ht_ref<'a, 'mcx>(node: &'a HashJoinState<'mcx>) -> &'a HashJoinTableData<'mcx> {
    node.hj_HashTable
        .as_deref()
        .expect("nodeHash: parallel seam needs a built hash table")
}

#[inline]
fn ht_mut<'a, 'mcx>(node: &'a mut HashJoinState<'mcx>) -> &'a mut HashJoinTableData<'mcx> {
    node.hj_HashTable
        .as_deref_mut()
        .expect("nodeHash: parallel seam needs a built hash table")
}

/// `BarrierPhase(&hashtable->parallel_state->build_barrier)`.
pub fn build_barrier_phase(node: &HashJoinState<'_>) -> i32 {
    let pstate = pstate_mut(ht_ref(node));
    barrier::BarrierPhase::call(&pstate.build_barrier)
}

/// `BarrierArriveAndWait(&pstate->build_barrier, wait_event)`.
pub fn build_barrier_arrive_and_wait(
    node: &mut HashJoinState<'_>,
    wait_event: uint32,
) -> PgResult<bool> {
    let pstate = pstate_mut(ht_mut(node));
    Ok(barrier::BarrierArriveAndWait::call(&mut pstate.build_barrier, wait_event))
}

/// `pg_atomic_fetch_add_u32(&pstate->distributor, 1)`.
pub fn parallel_distributor_next_start(node: &HashJoinState<'_>) -> u32 {
    let pstate = pstate_mut(ht_ref(node));
    pstate.distributor.value.fetch_add(1, Ordering::Relaxed)
}

/// `!hashtable->batches[batchno].done`.
pub fn parallel_batch_not_done(node: &HashJoinState<'_>, batchno: i32) -> bool {
    !ht_ref(node).batches[batchno as usize].done
}

/// `BarrierAttach(&batches[batchno].shared->batch_barrier)`.
pub fn parallel_batch_attach(node: &mut HashJoinState<'_>, batchno: i32) -> i32 {
    let ht = ht_mut(node);
    let dp = ht.batches[batchno as usize].shared;
    let batch = batch_shared_mut(ht, dp);
    barrier::BarrierAttach::call(&mut batch.batch_barrier)
}

/// `BarrierArriveAndWait(&batches[batchno].shared->batch_barrier, wait_event)`.
pub fn parallel_batch_arrive_and_wait(
    node: &mut HashJoinState<'_>,
    batchno: i32,
    wait_event: uint32,
) -> PgResult<bool> {
    let ht = ht_mut(node);
    let dp = ht.batches[batchno as usize].shared;
    let batch = batch_shared_mut(ht, dp);
    Ok(barrier::BarrierArriveAndWait::call(&mut batch.batch_barrier, wait_event))
}

/// `BarrierDetach(&batches[batchno].shared->batch_barrier)`.
pub fn parallel_batch_detach(node: &mut HashJoinState<'_>, batchno: i32) -> PgResult<()> {
    let ht = ht_mut(node);
    let dp = ht.batches[batchno as usize].shared;
    let batch = batch_shared_mut(ht, dp);
    barrier::BarrierDetach::call(&mut batch.batch_barrier);
    Ok(())
}

/// `BarrierPhase(&batches[batchno].shared->batch_barrier)`.
pub fn parallel_batch_phase(node: &HashJoinState<'_>, batchno: i32) -> i32 {
    let ht = ht_ref(node);
    let dp = ht.batches[batchno as usize].shared;
    let batch = batch_shared_mut(ht, dp);
    barrier::BarrierPhase::call(&batch.batch_barrier)
}

/// `hashtable->batches[batchno].done = true`.
pub fn parallel_batch_set_done(node: &mut HashJoinState<'_>, batchno: i32) -> PgResult<()> {
    ht_mut(node).batches[batchno as usize].done = true;
    Ok(())
}

/// `hashtable->curbatch = -1`.
pub fn parallel_set_curbatch_invalid(node: &mut HashJoinState<'_>) -> PgResult<()> {
    ht_mut(node).curbatch = -1;
    Ok(())
}

/// `hashtable->curbatch >= 0`.
pub fn parallel_has_curbatch(node: &HashJoinState<'_>) -> bool {
    ht_ref(node).curbatch >= 0
}

// ===========================================================================
//   Cross-module / cross-owner glue (calls into sibling modules and seams)
// ===========================================================================

/// `ExecHashGetBucketAndBatch` (hash_table.rs sibling module).
#[inline]
fn exec_hash_get_bucket_and_batch(
    hashtable: &HashJoinTableData<'_>,
    hashvalue: uint32,
) -> types_nodes::nodehash::BucketAndBatch {
    crate::hash_table::ExecHashGetBucketAndBatch(hashtable, hashvalue)
}

/// `&pstate->fileset` resolved as a SharedFileSetHandle for the sts seams. The
/// fileset lives inside the DSA-resident ParallelHashJoinState; its address is
/// the handle the shared-tuplestore owner expects.
#[inline]
fn pstate_fileset_handle(
    hashtable: &HashJoinTableData<'_>,
) -> types_execparallel::SharedFileSetHandle {
    let pstate = pstate_mut(hashtable);
    let addr = (&pstate.fileset as *const types_storage::fileset::SharedFileSet) as usize;
    types_execparallel::SharedFileSetHandle(addr)
}

/// `MyProcNumber` — the caller's per-backend proc number, passed to the lwlock
/// seam (the no-ambient-globals rule). Read off the parallel subsystem.
#[inline]
fn current_proc_number() -> types_core::ProcNumber {
    backend_access_transam_parallel_seams::parallel_worker_number::call() as types_core::ProcNumber
}

/// `sts_puttuple(accessor, &hashvalue, tuple)` over an on-DSA MinimalTuple at
/// `mintuple_addr`. The sts seam takes a real `MinimalTupleData`, so we view
/// the DSA bytes as one.
#[inline]
fn sts_puttuple_raw(
    accessor: types_execparallel::SharedTuplestoreAccessorHandle,
    hashvalue: uint32,
    mintuple_addr: usize,
    _t_len: usize,
) -> PgResult<()> {
    let meta = hashvalue.to_ne_bytes();
    let tuple = unsafe {
        &*(mintuple_addr as *const types_tuple::heaptuple::MinimalTupleData<'_>)
    };
    sts::sts_puttuple_handle::call(accessor, &meta, tuple)
}

/// `SHARED_TUPLESTORE_SINGLE_PASS` (sharedtuplestore.h).
const SHARED_TUPLESTORE_SINGLE_PASS: i32 = 0x01;

/// Build a zeroed `ParallelHashJoinBatchAccessor` with the given shared
/// pointer and tuplestore accessors (C palloc0's it, then sets these fields).
///
/// The canonical accessor stores its two `SharedTuplestoreAccessor *` as the
/// real (inherited-opacity) `PgBox<SharedTuplestoreAccessor>`; the
/// shared-tuplestore subsystem is unported, so the backend-local handle this
/// unit holds is carried inside the opaque accessor and unwrapped back to a
/// handle at the `*_handle` seam boundary (see [`accessor_handle`]).
#[inline]
fn new_accessor<'mcx>(
    mcx: Mcx<'mcx>,
    shared: DsaPointer,
    inner: Option<types_execparallel::SharedTuplestoreAccessorHandle>,
    outer: Option<types_execparallel::SharedTuplestoreAccessorHandle>,
) -> PgResult<types_nodes::nodehash::ParallelHashJoinBatchAccessor<'mcx>> {
    Ok(types_nodes::nodehash::ParallelHashJoinBatchAccessor {
        shared,
        preallocated: 0,
        ntuples: 0,
        size: 0,
        estimated_size: 0,
        old_ntuples: 0,
        at_least_one_chunk: false,
        outer_eof: false,
        done: false,
        inner_tuples: box_accessor(mcx, inner)?,
        outer_tuples: box_accessor(mcx, outer)?,
    })
}

/// Wrap a backend-local `SharedTuplestoreAccessorHandle` into the canonical
/// `PgBox<SharedTuplestoreAccessor>` field (the handle rides inside the opaque
/// accessor's `Any` payload). `None` stays the C `NULL`.
#[inline]
fn box_accessor<'mcx>(
    mcx: Mcx<'mcx>,
    h: Option<types_execparallel::SharedTuplestoreAccessorHandle>,
) -> PgResult<Option<mcx::PgBox<'mcx, types_nodes::nodehash::SharedTuplestoreAccessor>>> {
    match h {
        None => Ok(None),
        Some(handle) => {
            let acc = types_nodes::nodehash::SharedTuplestoreAccessor(
                types_nodes::Opaque(Some(Box::new(handle))),
            );
            Ok(Some(mcx::alloc_in(mcx, acc)?))
        }
    }
}

/// Recover the backend-local `SharedTuplestoreAccessorHandle` carried by a
/// canonical accessor field (the inverse of [`box_accessor`]).
#[inline]
fn accessor_handle(
    a: &Option<mcx::PgBox<'_, types_nodes::nodehash::SharedTuplestoreAccessor>>,
) -> Option<types_execparallel::SharedTuplestoreAccessorHandle> {
    a.as_ref().and_then(|b| {
        b.0 .0
            .as_ref()
            .and_then(|any| any.downcast_ref::<types_execparallel::SharedTuplestoreAccessorHandle>())
            .copied()
    })
}

/// `dsa_pointer_atomic_init(&buckets[i], val)` — initialize one bucket-head
/// atomic in the DSA-resident array.
#[inline]
fn dsa_pointer_atomic_init(buckets_addr: usize, i: i32, val: DsaPointer) {
    let p = (buckets_addr + i as usize * SIZEOF_DSA_POINTER_ATOMIC) as *mut pg_atomic_uint64;
    unsafe {
        core::ptr::write(p, pg_atomic_uint64::new(val));
    }
}

/// `dsa_pointer_atomic_write(&buckets[i], val)`.
#[inline]
fn dsa_pointer_atomic_write(buckets_addr: usize, i: i32, val: DsaPointer) {
    let p = (buckets_addr + i as usize * SIZEOF_DSA_POINTER_ATOMIC) as *const AtomicU64;
    unsafe {
        (*p).store(val, Ordering::Release);
    }
}

// ---- Executor slot / qual leaf operations. The on-DSA tuple is a flat
// MinimalTuple byte image; the slot<->tuple-image transfer (ExecFetchSlot-
// MinimalTuple / ExecStoreMinimalTuple) and qual evaluation are owned by
// execTuples/execExpr. The slot transfer + qual are reached through those
// owners' seam crates (loud until installed); the match-flag / per-tuple-reset
// pieces that are pure htup_details.h / context-reset logic are done inline. ----

// The on-DSA hash tuple is a flat MinimalTuple byte image carried by
// `HashJoinTupleRaw` + inline bytes. `mintuple_addr` is the address of that
// inline image. The slot the executor hands us (`TupleTableSlot`) is, in the
// current type model, a header-only carrier with no payload bytes; the real
// slot<->MinimalTuple transfer is owned by execTuples. Per-backend scratch
// holds the flat image for the duration of one insert/scan step.

/// Reconstruct a `MinimalTupleData` (into `mcx`) from the flat MinimalTuple
/// byte image at a DSA address. The image is the `to_minimal_bytes()` layout
/// the insert path wrote: leading `t_len` word then the body. C reads the same
/// bytes directly as a `MinimalTuple *`; the owned model rebuilds the struct.
#[inline]
fn mintuple_from_dsa<'mcx>(
    mcx: Mcx<'mcx>,
    mintuple_addr: usize,
) -> PgResult<types_tuple::heaptuple::MinimalTupleData<'mcx>> {
    // First word is t_len; the body length is implied by the staged image. Read
    // t_len, then the body bytes that follow (the to_minimal_bytes layout writes
    // exactly t_len does not bound the image, so derive the body from t_bits len).
    let t_len = unsafe { core::ptr::read_unaligned(mintuple_addr as *const u32) };
    // The staged image is: t_len(4) mt_padding(6) t_infomask2(2) t_infomask(2)
    // t_hoff(1) t_bits_len(4) t_bits[t_bits_len]. Read t_bits_len to size body.
    let bits_len = unsafe {
        core::ptr::read_unaligned((mintuple_addr + 4 + 6 + 2 + 2 + 1) as *const u32)
    } as usize;
    let body_len = 6 + 2 + 2 + 1 + 4 + bits_len;
    let body =
        unsafe { core::slice::from_raw_parts((mintuple_addr + 4) as *const u8, body_len) };
    types_tuple::heaptuple::MinimalTupleData::from_minimal_parts(mcx, t_len, body)
}

/// `work_mem` / `hash_mem_multiplier` GUCs (`utils/guc.c`). C per-backend
/// globals; ported as backend `thread_local` per AGENTS.md until the GUC owner
/// installs a setter. PG defaults: work_mem 4 MB (4096 KB), multiplier 2.0.
#[inline]
pub(crate) fn hash_mem_gucs() -> (i32, f64) {
    HASH_MEM_GUCS.with(|g| *g.borrow())
}

std::thread_local! {
    static HASH_MEM_GUCS: std::cell::RefCell<(i32, f64)> = const { std::cell::RefCell::new((4096, 2.0)) };
}

// Local `Vec`/format shims (the crate is `std`).
mod alloc_vec {
    pub use std::vec::Vec;
}
mod alloc_format {
    pub fn format(args: core::fmt::Arguments<'_>) -> String {
        std::fmt::format(args)
    }
}
