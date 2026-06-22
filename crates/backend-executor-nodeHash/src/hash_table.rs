//! The serial in-memory hash table: create / size / build / probe / grow /
//! reset, plus the dense allocator and the memory-limit helper.
//!
//! OWNED-MODEL TRANSLATION (see `types_nodes::nodehash`): C carves each
//! `HashJoinTupleData` header + its inline `MinimalTuple` out of the
//! dense-allocation chunk byte buffers in `batchCxt` and chains them by raw
//! pointer; the owned model stores every in-memory tuple once in the table's
//! `tuples` arena and chains them by [`HashTupleIdx`]. The chunk headers live
//! in the `chunk_arena`, linked by [`HashChunkIdx`]. `dense_alloc` does the C
//! chunk byte-accounting (which drives `spaceUsed`) AND reserves the arena
//! slot the caller fills, returning its index — the owned analog of C's
//! `char *` into the chunk buffer.

use mcx::{Mcx, PgBox, PgVec};
use types_core::{uint32, Size};
use types_error::PgResult;
use types_nodes::nodehash::{
    BucketAndBatch, HashJoinBuckets, HashJoinTupleData, HashJoinTupleLink, HashMemoryChunkData,
    HashMemoryChunkLink, HashChunkIdx, HashJoinState, HashState, HashJoinTableData, HashTupleIdx,
    INVALID_SKEW_BUCKET_NO, HASH_CHUNK_SIZE, HASH_CHUNK_THRESHOLD,
};
use types_nodes::nodehash::Hash as HashPlan;
use types_nodes::nodehash::ParallelHashJoinState;
use types_tuple::backend_access_common_heaptuple::FormedMinimalTuple;
use types_tuple::heaptuple::{MinimalTupleData, HEAP_TUPLE_HAS_MATCH};

/// Serialize a [`FormedMinimalTuple`] to its contiguous C `MinimalTuple` byte
/// image (the flat blob, `t_len` first) — the form the batch temp file / shared
/// tuplestore boundary carries. A well-formed in-arena tuple can only fail on
/// the allocation `ereport(ERROR)` (OOM).
pub(crate) fn mintuple_to_flat<'mcx>(
    mcx: Mcx<'mcx>,
    mtup: &FormedMinimalTuple<'_>,
) -> PgResult<PgVec<'mcx, u8>> {
    use backend_access_common_heaptuple::flat::MinimalTupleFlatError;
    match backend_access_common_heaptuple::flat::minimal_tuple_to_flat(mcx, mtup) {
        Ok(blob) => Ok(blob),
        Err(MinimalTupleFlatError::Pg(err)) => Err(err),
        Err(other) => panic!("minimal_tuple_to_flat on an in-arena tuple failed: {other:?}"),
    }
}

use crate::{
    BLCKSZ, HJTUPLE_OVERHEAD, MAXALIGN, MaxAllocSize, SizeofMinimalTupleHeader, SKEW_BUCKET_OVERHEAD,
};

// ===========================================================================
//                  Constants / bit utilities (local C macros)
// ===========================================================================

/// `NTUP_PER_BUCKET` (nodeHash.c) — target bucket loading (tuples per bucket).
const NTUP_PER_BUCKET: i32 = 1;

/// `INT_MAX` (limits.h).
const INT_MAX: i32 = i32::MAX;
/// `SIZE_MAX` (stdint.h).
const SIZE_MAX: usize = usize::MAX;
/// `SKEW_HASH_MEM_PERCENT` (hashjoin.h).
const SKEW_HASH_MEM_PERCENT_USIZE: usize = 2;

/// `sizeof(HashJoinTuple)` — a `HashJoinTupleData *` pointer (8 bytes on 64-bit
/// PostgreSQL). The bucket array and the sizing math count one per bucket.
const SIZEOF_HASHJOINTUPLE_PTR: usize = 8;
/// `sizeof(HashSkewBucket *)` — 8 bytes on 64-bit.
const SIZEOF_HASHSKEWBUCKET_PTR: usize = 8;
/// `sizeof(int)` — 4 bytes.
const SIZEOF_INT: usize = 4;

/// `pg_nextpower2_32(num)` (pg_bitutils.h) — the smallest power of two greater
/// than or equal to `num` (undefined for 0 / values past `PG_UINT32_MAX/2 + 1`).
#[inline]
fn pg_nextpower2_32(num: u32) -> u32 {
    debug_assert!(num > 0, "pg_nextpower2_32 is undefined for 0");
    if num <= 1 {
        return 1;
    }
    1u32 << (32 - (num - 1).leading_zeros())
}

/// `pg_nextpower2_size_t(num)` (pg_bitutils.h) — smallest power of two ≥ `num`.
#[inline]
fn pg_nextpower2_size_t(num: usize) -> usize {
    debug_assert!(num > 0, "pg_nextpower2_size_t is undefined for 0");
    if num <= 1 {
        return 1;
    }
    1usize << (usize::BITS - (num - 1).leading_zeros())
}

/// `pg_prevpower2_size_t(num)` (pg_bitutils.h) — largest power of two ≤ `num`
/// (undefined for 0).
#[inline]
fn pg_prevpower2_size_t(num: usize) -> usize {
    debug_assert!(num > 0, "pg_prevpower2_size_t is undefined for 0");
    if num == 0 {
        return 0;
    }
    1usize << (usize::BITS - 1 - num.leading_zeros())
}

/// `pg_rotate_right32(word, n)` (pg_bitutils.h).
#[inline]
fn pg_rotate_right32(word: u32, n: u32) -> u32 {
    word.rotate_right(n)
}

/// `my_log2(num)` (dynahash.c, used by nodeHash) — `ceil(log2(num))`. For a
/// power-of-2 `num`, this is the exact exponent. Implemented as
/// `pg_leftmost_one_pos(num - 1) + 1` (the C `my_log2`). Used by the full
/// `ExecHashTableCreate` body (`log2_nbuckets = my_log2(nbuckets)`), which is
/// past the unreachable plan-node-access seam boundary today.
#[inline]
#[allow(dead_code)]
fn my_log2(num: i64) -> i32 {
    // guard against too-large input
    let num = num.max(1);
    // ceil(log2(num)) = position of leftmost 1 in (num - 1), plus 1
    if num <= 1 {
        return 0;
    }
    (64 - ((num - 1) as u64).leading_zeros()) as i32
}

// ===========================================================================
//                       ExecChooseHashTableSize result
// ===========================================================================

/// Out-parameters of `ExecChooseHashTableSize` (nodeHash.c:658) — C writes
/// through `space_allowed` / `numbuckets` / `numbatches` / `num_skew_mcvs`.
#[derive(Clone, Copy, Debug)]
pub struct HashTableSize {
    /// `*space_allowed`.
    pub space_allowed: Size,
    /// `*numbuckets`.
    pub numbuckets: i32,
    /// `*numbatches`.
    pub numbatches: i32,
    /// `*num_skew_mcvs`.
    pub num_skew_mcvs: i32,
}

// ===========================================================================
//   Owned-model bucket-array allocation (palloc0_array(HashJoinTuple, n))
// ===========================================================================

/// Allocate `nbuckets` empty (`None`) bucket-head slots as arena indices,
/// bounded by `MaxAllocSize / sizeof(HashJoinTuple *)` so an oversized request
/// is a recoverable OOM error (`mcx.oom`), never an abort. Mirrors
/// `palloc0_array(HashJoinTuple, nbuckets)`.
fn alloc_empty_bucket_indices<'mcx>(
    mcx: Mcx<'mcx>,
    nbuckets: i32,
) -> PgResult<PgVec<'mcx, Option<HashTupleIdx>>> {
    let n = nbuckets.max(0) as usize;
    let bytes = n.saturating_mul(SIZEOF_HASHJOINTUPLE_PTR);
    if n > MaxAllocSize / SIZEOF_HASHJOINTUPLE_PTR {
        return Err(mcx.oom(bytes));
    }
    let mut v: PgVec<'mcx, Option<HashTupleIdx>> = PgVec::new_in(mcx);
    if v.try_reserve_exact(n).is_err() {
        return Err(mcx.oom(bytes));
    }
    v.resize_with(n, || None);
    Ok(v)
}

// ===========================================================================
//                          ExecHashTableCreate
// ===========================================================================

/// `ExecHashTableCreate(HashState *state)` (nodeHash.c:446) — create an empty
/// hashtable data structure for hashjoin. Allocates the table and its child
/// contexts in `mcx`.
pub fn ExecHashTableCreate<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut HashState<'mcx>,
    estate: &types_nodes::EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, HashJoinTableData<'mcx>>> {
    // node = (Hash *) state->ps.plan;  outerNode = outerPlan(node);
    //
    // `state->ps.plan` is the inner `Hash` plan node; `outerPlan(node)` is its
    // left subplan (the inner-relation source). Read the planner's row/width
    // estimates and skew gating off the real plan tree.
    let hash_node: &HashPlan<'mcx> = match state.ps.plan {
        Some(p) => p.expect_hash(),
        None => panic!("ExecHashTableCreate: state.ps.plan is NULL"),
    };
    let outer_node = hash_node
        .plan
        .lefttree
        .as_deref()
        .expect("ExecHashTableCreate: outerPlan(node) must be present");
    let outer_plan = outer_node.plan_head();

    // If this is a shared hash table with a partial plan, we can't use
    // outerNode->plan_rows; use the planner's total-rows estimate instead.
    let rows = if hash_node.plan.parallel_aware {
        hash_node.rows_total
    } else {
        outer_plan.plan_rows
    };

    let skew_table = hash_node.skewTable;
    let parallel = state.parallel_state.is_some();
    // state->parallel_state->nparticipants - 1 (parallel only); 0 in serial.
    // In C, `state->parallel_state` is a `ParallelHashJoinState *` — a plain
    // backend-local pointer into the DSM segment obtained by `shm_toc_lookup`
    // (worker) / `shm_toc_allocate` (leader); `nparticipants` is a direct
    // field deref, NOT a dsa_pointer resolution. The owned model stores that
    // same backend-local segment address in `parallel_state` (see
    // `ExecHashJoin{Initialize,}Worker`/`InitializeDSM`, which write
    // `chunk.0`), so deref it directly here. Resolving it through
    // `dsa_get_address` (treating the raw segment address as a dsa_pointer)
    // splits it into a bogus segment-index/offset and reads garbage — the
    // immediate cause of a 0 `nparticipants` and the downstream
    // `pg_prevpower2_size_t is undefined for 0` panic. Serial joins (no
    // parallel_state) pass 0.
    let parallel_workers = match state.parallel_state {
        Some(dp) => {
            // SAFETY: `dp` is the backend-local address of the live
            // ParallelHashJoinState in the attached DSM segment (valid for the
            // duration of the join, the C invariant); we only read the scalar
            // nparticipants field.
            let pstate =
                unsafe { &*(dp as usize as *const ParallelHashJoinState) };
            pstate.nparticipants - 1
        }
        None => 0,
    };

    let HashTableSize {
        space_allowed,
        numbuckets: nbuckets,
        numbatches: nbatch,
        num_skew_mcvs,
    } = ExecChooseHashTableSize(
        rows,
        outer_plan.plan_width,
        types_core::OidIsValid(skew_table),
        parallel,
        parallel_workers,
    );

    // nbuckets must be a power of 2.
    let log2_nbuckets = my_log2(nbuckets as i64);
    debug_assert_eq!(nbuckets, 1 << log2_nbuckets);

    // Initialize the hash table control block. The control block is allocated
    // from the per-query context (mcx); the working storage lives in the
    // subsidiary hash/batch/spill contexts (here all modelled by mcx, since the
    // owned arenas are dropped with the table).
    let spaceAllowedSkew = space_allowed * SKEW_HASH_MEM_PERCENT_USIZE / 100;
    let mut hashtable = mcx::alloc_in(
        mcx,
        HashJoinTableData {
            nbuckets,
            log2_nbuckets,
            nbuckets_original: nbuckets,
            nbuckets_optimal: nbuckets,
            log2_nbuckets_optimal: log2_nbuckets,
            buckets: HashJoinBuckets::Unshared(PgVec::new_in(mcx)),
            skewEnabled: false,
            skewBucket: PgVec::new_in(mcx),
            skewBucketLen: 0,
            nSkewBuckets: 0,
            skewBucketNums: PgVec::new_in(mcx),
            nbatch,
            curbatch: 0,
            nbatch_original: nbatch,
            nbatch_outstart: nbatch,
            growEnabled: true,
            totalTuples: 0.0,
            partialTuples: 0.0,
            skewTuples: 0.0,
            innerBatchFile: PgVec::new_in(mcx),
            outerBatchFile: PgVec::new_in(mcx),
            spaceUsed: 0,
            spaceAllowed: space_allowed,
            spacePeak: 0,
            spaceUsedSkew: 0,
            spaceAllowedSkew,
            chunks: None,
            current_chunk: None,
            // hashtable->area = state->ps.state->es_query_dsa;
            // C reaches the per-query DSA area through the Hash node's EState
            // back-link; the owned model threads the live EState explicitly, so
            // read es_query_dsa off it directly (the same object
            // ParallelQueryMain sets es_query_dsa on before the executor run).
            // None for a serial join (es_query_dsa is NULL outside parallel).
            area: estate.es_query_dsa,
            parallel_state: state.parallel_state,
            batches: PgVec::new_in(mcx),
            current_chunk_shared: types_execparallel::DsaPointer::default(),
            tuples: PgVec::new_in(mcx),
            chunk_arena: PgVec::new_in(mcx),
            // spillCxt is a child of hashCxt in C; modelled by the per-query
            // context the batch spill files are charged to.
            spillCxt: mcx,
        },
    )?;

    if nbatch > 1 && hashtable.parallel_state.is_none() {
        // Allocate and initialize the file arrays in spillCxt (not needed for
        // the parallel case, which uses shared tuplestores instead of raw
        // files). The files are not opened until needed.
        hashtable.innerBatchFile = {
            let mut v = PgVec::new_in(mcx);
            v.resize_with(nbatch as usize, || None);
            v
        };
        hashtable.outerBatchFile = {
            let mut v = PgVec::new_in(mcx);
            v.resize_with(nbatch as usize, || None);
            v
        };
        // ... but make sure we have temp tablespaces established for them.
        //   PrepareTempTablespaces();
        // Routed through the tablespace owner's seam, exactly as buffile.c /
        // tuplestore.c (the other in-tree consumers) do.
        backend_commands_tablespace_seams::prepare_temp_tablespaces::call()?;
    }

    if hashtable.parallel_state.is_some() {
        // Attach to the build barrier, elect a backend to set up the shared
        // batch state, allocate batch 0. All of this lives in the DSA-resident
        // shared state reached through the (unported) DSA area; the parallel
        // setup routines raise a recoverable not-ported ERROR until
        // sharedtuplestore/sharedfileset land.
        crate::parallel::ExecParallelHashJoinSetUpBatches(mcx, &mut hashtable, nbatch)?;
        crate::parallel::ExecParallelHashTableAlloc(mcx, &mut hashtable, 0)?;
    } else {
        // Serial: allocate the bucket array and set each bucket empty.
        hashtable.buckets =
            HashJoinBuckets::Unshared(alloc_empty_bucket_indices(mcx, nbuckets)?);

        // Set up skew optimization, if possible and there's a need for more
        // than one batch. (In a one-batch join, there's no point.)
        if nbatch > 1 {
            crate::skew::ExecHashBuildSkewHash(mcx, state, &mut hashtable, hash_node, num_skew_mcvs)?;
        }
    }

    Ok(hashtable)
}

// ===========================================================================
//                        ExecChooseHashTableSize
// ===========================================================================

/// `ExecChooseHashTableSize(ntuples, tupwidth, useskew, try_combined_hash_mem,
/// parallel_workers, ...)` (nodeHash.c:658) — compute the bucket/batch/skew
/// dimensions for the planned input. Pure arithmetic (no allocation; the two
/// memory GUCs are passed explicitly through `get_hash_memory_limit`).
pub fn ExecChooseHashTableSize(
    ntuples: f64,
    tupwidth: i32,
    useskew: bool,
    try_combined_hash_mem: bool,
    parallel_workers: i32,
) -> HashTableSize {
    choose_hash_table_size(
        ntuples,
        tupwidth,
        useskew,
        try_combined_hash_mem,
        parallel_workers,
        get_hash_memory_limit_fixed(),
    )
}

/// Inner sizing routine threading the memory limit explicitly so the recursive
/// `try_combined_hash_mem` fall-back uses the same (already-computed) GUC base.
fn choose_hash_table_size(
    mut ntuples: f64,
    tupwidth: i32,
    useskew: bool,
    try_combined_hash_mem: bool,
    parallel_workers: i32,
    base_hash_mem: Size,
) -> HashTableSize {
    let mut nbatch: i32 = 1;

    // Force a plausible relation size if no info.
    if ntuples <= 0.0 {
        ntuples = 1000.0;
    }

    // Estimate tupsize based on footprint of tuple in hashtable.
    let tupsize =
        HJTUPLE_OVERHEAD + MAXALIGN(SizeofMinimalTupleHeader) + MAXALIGN(tupwidth.max(0) as usize);
    let inner_rel_bytes = ntuples * tupsize as f64;

    // Compute in-memory hashtable size limit from GUCs.
    let mut hash_table_bytes: Size = base_hash_mem;

    // Parallel Hash tries to use the combined hash_mem of all workers.
    if try_combined_hash_mem {
        // Careful, this could overflow size_t.
        let mut newlimit = hash_table_bytes as f64 * (parallel_workers + 1) as f64;
        newlimit = newlimit.min(SIZE_MAX as f64);
        hash_table_bytes = newlimit as Size;
    }

    let mut space_allowed: Size = hash_table_bytes;

    // If skew optimization is possible, estimate the number of skew buckets.
    let num_skew_mcvs: i32;
    if useskew {
        // Compute number of MCVs we could hold in hash_table_bytes.
        let bytes_per_mcv =
            tupsize + (8 * SIZEOF_HASHSKEWBUCKET_PTR) + SIZEOF_INT + SKEW_BUCKET_OVERHEAD;
        let mut skew_mcvs = hash_table_bytes / bytes_per_mcv;

        // Now scale by SKEW_HASH_MEM_PERCENT.
        skew_mcvs = (skew_mcvs * SKEW_HASH_MEM_PERCENT_USIZE) / 100;

        // Now clamp to integer range.
        skew_mcvs = skew_mcvs.min(INT_MAX as usize);

        num_skew_mcvs = skew_mcvs as i32;

        // Reduce hash_table_bytes by the amount needed for the skew table.
        if skew_mcvs > 0 {
            hash_table_bytes -= skew_mcvs * bytes_per_mcv;
        }
    } else {
        num_skew_mcvs = 0;
    }
    let mut num_skew_mcvs = num_skew_mcvs;

    // Set nbuckets to achieve an average bucket load of NTUP_PER_BUCKET.
    let mut max_pointers = hash_table_bytes / SIZEOF_HASHJOINTUPLE_PTR;
    max_pointers = max_pointers.min(MaxAllocSize / SIZEOF_HASHJOINTUPLE_PTR);
    // If max_pointers isn't a power of 2, must round it down to one.
    max_pointers = pg_prevpower2_size_t(max_pointers);
    // Also ensure we avoid integer overflow in nbatch and nbuckets.
    max_pointers = max_pointers.min((INT_MAX / 2 + 1) as usize);

    let mut dbuckets = (ntuples / NTUP_PER_BUCKET as f64).ceil();
    dbuckets = dbuckets.min(max_pointers as f64);
    let mut nbuckets = dbuckets as i32;
    // don't let nbuckets be really small, though ...
    nbuckets = nbuckets.max(1024);
    // ... and force it to be a power of 2.
    nbuckets = pg_nextpower2_32(nbuckets as u32) as i32;

    // If there's not enough space to store the projected number of tuples and
    // the required bucket headers, we will need multiple batches.
    let mut bucket_bytes = SIZEOF_HASHJOINTUPLE_PTR * nbuckets as usize;
    if inner_rel_bytes + bucket_bytes as f64 > hash_table_bytes as f64 {
        // We'll need multiple batches.
        //
        // If Parallel Hash with combined hash_mem would still need multiple
        // batches, we'll have to fall back to regular hash_mem budget.
        if try_combined_hash_mem {
            return choose_hash_table_size(
                ntuples,
                tupwidth,
                useskew,
                false,
                parallel_workers,
                base_hash_mem,
            );
        }

        // Estimate the number of buckets we'll want to have when hash_mem is
        // entirely full.
        let bucket_size = tupsize * NTUP_PER_BUCKET as usize + SIZEOF_HASHJOINTUPLE_PTR;
        let mut sbuckets: usize;
        if hash_table_bytes <= bucket_size {
            sbuckets = 1; // avoid pg_nextpower2_size_t(0)
        } else {
            sbuckets = pg_nextpower2_size_t(hash_table_bytes / bucket_size);
        }
        sbuckets = sbuckets.min(max_pointers);
        nbuckets = sbuckets as i32;
        nbuckets = pg_nextpower2_32(nbuckets as u32) as i32;
        bucket_bytes = nbuckets as usize * SIZEOF_HASHJOINTUPLE_PTR;

        debug_assert!(bucket_bytes <= hash_table_bytes / 2);

        // Calculate required number of batches.
        let mut dbatch = (inner_rel_bytes / (hash_table_bytes - bucket_bytes) as f64).ceil();
        dbatch = dbatch.min(max_pointers as f64);
        let minbatch = dbatch as i32;
        nbatch = pg_nextpower2_32(minbatch.max(2) as u32) as i32;
    }

    // Optimize the total amount of memory consumed by the hash node.
    while nbatch > 1 {
        // Check that buckets won't overflow MaxAllocSize.
        if nbuckets as usize > (MaxAllocSize / SIZEOF_HASHJOINTUPLE_PTR / 2) {
            break;
        }

        // num_skew_mcvs should be less than nbuckets.
        debug_assert!(num_skew_mcvs < (INT_MAX / 2));

        // Check that space_allowed won't overflow SIZE_MAX.
        if space_allowed > (SIZE_MAX / 2) {
            break;
        }

        // Will halving the number of batches and doubling the size of the
        // hashtable reduce overall memory usage?
        if (nbatch as usize) < space_allowed / BLCKSZ {
            break;
        }

        nbuckets *= 2;

        num_skew_mcvs *= 2;
        space_allowed *= 2;

        nbatch /= 2;
    }

    debug_assert!(nbuckets > 0);
    debug_assert!(nbatch > 0);

    HashTableSize {
        space_allowed,
        numbuckets: nbuckets,
        numbatches: nbatch,
        num_skew_mcvs,
    }
}

// ===========================================================================
//                          ExecHashTableDestroy
// ===========================================================================

/// `ExecHashTableDestroy(HashJoinTable hashtable)` (nodeHash.c:956) — destroy a
/// hash table, closing its batch temp files. Fallible (`BufFileClose` I/O).
///
/// OWNED-MODEL TRANSLATION: C closes the per-batch temp files (skipping batch
/// 0), then `MemoryContextDelete(hashCxt)` frees all subsidiary storage, then
/// `pfree`s the control block. The owned model closes the temp files through
/// the buffile seam, then drops the per-batch arenas / bucket array; the
/// `HashJoinTableData` control block (`PgBox`) is dropped on return.
pub fn ExecHashTableDestroy<'mcx>(
    mut hashtable: PgBox<'mcx, HashJoinTableData<'mcx>>,
) -> PgResult<()> {
    // Make sure all the temp files are closed. We skip batch 0, since it can't
    // have any temp files (and the arrays might not even exist if nbatch is
    // only 1). Parallel hash joins don't use these files.
    if !hashtable.innerBatchFile.is_empty() {
        let nbatch = hashtable.nbatch;
        for i in 1..nbatch as usize {
            // if (hashtable->innerBatchFile[i]) BufFileClose(...);
            if let Some(file) = hashtable.innerBatchFile[i].take() {
                backend_storage_file_buffile_seams::buf_file_close::call(file)?;
            }
            if let Some(file) = hashtable.outerBatchFile[i].take() {
                backend_storage_file_buffile_seams::buf_file_close::call(file)?;
            }
        }
    }

    // Release working memory (batchCxt is a child of hashCxt, so it goes away
    // too): drop the per-batch arenas and the bucket array. The control block
    // itself (`PgBox`) is dropped when `hashtable` goes out of scope here.
    hashtable.tuples.clear();
    hashtable.chunk_arena.clear();
    hashtable.chunks = None;
    hashtable.current_chunk = None;

    Ok(())
}

// ===========================================================================
//                       ExecHashIncreaseBatchSize
// ===========================================================================

/// `ExecHashIncreaseBatchSize(HashJoinTable hashtable)` (nodeHash.c:998) — if
/// doubling the in-memory hash table would use less memory than doubling the
/// number of batches, double `spaceAllowed` and return `true`; otherwise
/// return `false` (and the caller adds batches). Returns the C `bool`.
pub fn ExecHashIncreaseBatchSize<'mcx>(hashtable: &mut HashJoinTableData<'mcx>) -> bool {
    // How much additional memory would doubling nbatch use? Each batch may
    // require two buffered files (inner/outer), with a BLCKSZ buffer.
    let batch_space: Size = hashtable.nbatch as usize * 2 * BLCKSZ;

    // Compare the new space needed for doubling nbatch and for enlarging the
    // in-memory hash table.
    if hashtable.spaceAllowed <= batch_space {
        hashtable.spaceAllowed *= 2;
        true
    } else {
        false
    }
}

// ===========================================================================
//                      ExecHashIncreaseNumBatches
// ===========================================================================

/// `ExecHashIncreaseNumBatches(HashJoinTable hashtable)` (nodeHash.c:1030) —
/// double the number of batches when the in-memory table grew too large,
/// rescanning and dumping moved tuples to their batch files.
pub fn ExecHashIncreaseNumBatches<'mcx>(
    mcx: Mcx<'mcx>,
    hashtable: &mut HashJoinTableData<'mcx>,
) -> PgResult<()> {
    let oldnbatch = hashtable.nbatch;
    let curbatch = hashtable.curbatch;

    // do nothing if we've decided to shut off growth
    if !hashtable.growEnabled {
        return Ok(());
    }

    // safety check to avoid overflow
    //   if (oldnbatch > Min(INT_MAX / 2, MaxAllocSize / (sizeof(void *) * 2)))
    let safety = (INT_MAX / 2).min((MaxAllocSize / (8 * 2)) as i32);
    if oldnbatch > safety {
        return Ok(());
    }

    // consider increasing size of the in-memory hash table instead
    if ExecHashIncreaseBatchSize(hashtable) {
        return Ok(());
    }

    let nbatch = oldnbatch * 2;
    debug_assert!(nbatch > 1);

    if hashtable.innerBatchFile.is_empty() {
        // we had no file arrays before
        //   hashtable->innerBatchFile = palloc0_array(BufFile *, nbatch);
        //   hashtable->outerBatchFile = palloc0_array(BufFile *, nbatch);
        let mut inner = PgVec::new_in(mcx);
        if inner.try_reserve_exact(nbatch as usize).is_err() {
            return Err(mcx.oom(nbatch as usize * 8));
        }
        inner.resize_with(nbatch as usize, || None);
        let mut outer = PgVec::new_in(mcx);
        if outer.try_reserve_exact(nbatch as usize).is_err() {
            return Err(mcx.oom(nbatch as usize * 8));
        }
        outer.resize_with(nbatch as usize, || None);
        hashtable.innerBatchFile = inner;
        hashtable.outerBatchFile = outer;

        // time to establish the temp tablespaces, too
        //   PrepareTempTablespaces();
        // Routed through the tablespace owner's seam, exactly as buffile.c /
        // tuplestore.c (the other in-tree consumers) do.
        let _ = mcx;
        backend_commands_tablespace_seams::prepare_temp_tablespaces::call()?;
    } else {
        // enlarge arrays and zero out added entries
        //   repalloc0_array(innerBatchFile, BufFile *, oldnbatch, nbatch);
        //   repalloc0_array(outerBatchFile, BufFile *, oldnbatch, nbatch);
        if hashtable.innerBatchFile.try_reserve(nbatch as usize - oldnbatch as usize).is_err() {
            return Err(mcx.oom(nbatch as usize * 8));
        }
        hashtable.innerBatchFile.resize_with(nbatch as usize, || None);
        if hashtable.outerBatchFile.try_reserve(nbatch as usize - oldnbatch as usize).is_err() {
            return Err(mcx.oom(nbatch as usize * 8));
        }
        hashtable.outerBatchFile.resize_with(nbatch as usize, || None);
    }

    hashtable.nbatch = nbatch;

    // Scan through the existing hash table entries and dump out any that are no
    // longer of the current batch.
    let mut ninmemory: i64 = 0;
    let mut nfreed: i64 = 0;

    // If we know we need to resize nbuckets, we can do it while rebatching.
    if hashtable.nbuckets_optimal != hashtable.nbuckets {
        // we never decrease the number of buckets
        debug_assert!(hashtable.nbuckets_optimal > hashtable.nbuckets);

        hashtable.nbuckets = hashtable.nbuckets_optimal;
        hashtable.log2_nbuckets = hashtable.log2_nbuckets_optimal;

        //   hashtable->buckets.unshared =
        //       repalloc_array(buckets.unshared, HashJoinTuple, nbuckets);
        // (the memset below re-zeroes it, so reallocate empty.)
        let buckets = alloc_empty_bucket_indices(mcx, hashtable.nbuckets)?;
        hashtable.buckets = HashJoinBuckets::Unshared(buckets);
    } else {
        // memset(buckets.unshared, 0, sizeof(HashJoinTuple) * nbuckets);
        match &mut hashtable.buckets {
            HashJoinBuckets::Unshared(b) => {
                for slot in b.iter_mut() {
                    *slot = None;
                }
            }
            HashJoinBuckets::Shared(_) => {
                // Serial rebatch never runs on a shared bucket array.
                unreachable!("ExecHashIncreaseNumBatches with shared buckets (nodeHash.c:1030)")
            }
        }
    }

    // We will scan through the chunks directly, so that we can reset the
    // buckets now and not have to track which tuples have already been
    // processed. C frees the old chunks as it goes and re-stages kept tuples
    // into fresh dense chunks.
    //
    // OWNED-MODEL TRANSLATION: C walks `HASH_CHUNK_DATA(chunk) + idx` over each
    // old chunk's byte buffer to visit every in-memory tuple. The owned arena
    // already holds exactly those current-batch in-memory tuples once, so we
    // walk the `tuples` arena directly (the same equivalence
    // `ExecHashIncreaseNumBuckets` uses). We snapshot the current tuples, reset
    // the arena + chunk list, then re-stage the kept tuples via `dense_alloc`
    // (rebuilding both the dense chunks and the bucket chains), and dump the
    // moved-out tuples through the inner-batch-file save seam.
    let old_tuples: PgVec<'mcx, HashJoinTupleData<'mcx>> =
        core::mem::replace(&mut hashtable.tuples, PgVec::new_in(mcx));
    hashtable.chunk_arena.clear();
    hashtable.chunks = None;
    hashtable.current_chunk = None;

    for old in old_tuples.into_iter() {
        let hash_tuple_size = HJTUPLE_OVERHEAD + old.mintuple.tuple.t_len as usize;

        ninmemory += 1;
        let bb = ExecHashGetBucketAndBatch(hashtable, old.hashvalue);
        let bucketno = bb.bucketno;
        let batchno = bb.batchno;

        if batchno == curbatch {
            // keep tuple in memory - copy it into the new chunk
            //   copyTuple = (HashJoinTuple) dense_alloc(hashtable, hashTupleSize);
            //   memcpy(copyTuple, hashTuple, hashTupleSize);
            let copy_idx = dense_alloc(mcx, hashtable, hash_tuple_size)?;
            let hashvalue = old.hashvalue;
            let mintuple = old.mintuple;
            {
                let dst = &mut hashtable.tuples[copy_idx.0];
                dst.hashvalue = hashvalue;
                dst.mintuple = mintuple;
            }

            // and add it back to the appropriate bucket
            //   copyTuple->next.unshared = hashtable->buckets.unshared[bucketno];
            //   hashtable->buckets.unshared[bucketno] = copyTuple;
            match &mut hashtable.buckets {
                HashJoinBuckets::Unshared(heads) => {
                    let old_head = heads[bucketno as usize];
                    hashtable.tuples[copy_idx.0].next = HashJoinTupleLink::Unshared(old_head);
                    heads[bucketno as usize] = Some(copy_idx);
                }
                HashJoinBuckets::Shared(_) => {
                    unreachable!("serial rebatch with shared buckets (nodeHash.c:1030)")
                }
            }
        } else {
            // dump it out
            debug_assert!(batchno > curbatch);
            //   ExecHashJoinSaveTuple(HJTUPLE_MINTUPLE(hashTuple),
            //                         hashTuple->hashvalue,
            //                         &hashtable->innerBatchFile[batchno],
            //                         hashtable);
            let hashvalue = old.hashvalue;
            // ExecHashJoinSaveTuple writes the minimal tuple's contiguous C byte
            // image (the flat blob, `t_len` bytes) to the batch temp file.
            let blob = mintuple_to_flat(mcx, &old.mintuple)?;
            let file = &mut hashtable.innerBatchFile[batchno as usize];
            backend_executor_nodeHashjoin_seams::ExecHashJoinSaveTuple::call(
                mcx,
                &blob,
                hashvalue,
                file,
            )?;

            hashtable.spaceUsed -= hash_tuple_size;
            nfreed += 1;
        }

        // CHECK_FOR_INTERRUPTS(): owned by the signal/interrupt subsystem; the
        // C cancellation site is a documented note (a fake check would silently
        // drop cancellation).
    }

    // If we dumped out either all or none of the tuples in the table, disable
    // further expansion of nbatch. This implies enough tuples of identical
    // hashvalues to overflow spaceAllowed; subdividing further cannot help.
    if nfreed == 0 || nfreed == ninmemory {
        hashtable.growEnabled = false;
    }

    Ok(())
}

// ===========================================================================
//                      ExecHashIncreaseNumBuckets
// ===========================================================================

/// `ExecHashIncreaseNumBuckets(HashJoinTable hashtable)` (nodeHash.c:1587) —
/// double the bucket count and reinsert every in-memory tuple.
pub fn ExecHashIncreaseNumBuckets<'mcx>(
    mcx: Mcx<'mcx>,
    hashtable: &mut HashJoinTableData<'mcx>,
) -> PgResult<()> {
    // do nothing if not an increase (it's called increase for a reason)
    if hashtable.nbuckets >= hashtable.nbuckets_optimal {
        return Ok(());
    }

    hashtable.nbuckets = hashtable.nbuckets_optimal;
    hashtable.log2_nbuckets = hashtable.log2_nbuckets_optimal;

    debug_assert!(hashtable.nbuckets > 1);
    debug_assert!(hashtable.nbuckets <= (INT_MAX / 2));
    debug_assert!(hashtable.nbuckets == (1 << hashtable.log2_nbuckets));

    // Just reallocate the proper number of buckets - we don't need to walk
    // through them - we can walk the dense-allocated chunks. The owned model
    // walks the `tuples` arena (every in-memory tuple) directly instead of the
    // chunk byte buffers; that is equivalent because the arena holds exactly
    // the current-batch in-memory tuples and the per-tuple `next` chain is
    // about to be rebuilt from scratch.
    //   hashtable->buckets.unshared =
    //       repalloc_array(buckets.unshared, HashJoinTuple, nbuckets);
    //   memset(buckets.unshared, 0, nbuckets * sizeof(HashJoinTuple));
    let buckets = alloc_empty_bucket_indices(mcx, hashtable.nbuckets)?;
    hashtable.buckets = HashJoinBuckets::Unshared(buckets);

    // scan through all tuples in all chunks to rebuild the hash table
    let ntuples = hashtable.tuples.len();
    for i in 0..ntuples {
        let hashvalue = hashtable.tuples[i].hashvalue;
        let bb = ExecHashGetBucketAndBatch(hashtable, hashvalue);
        let bucketno = bb.bucketno;

        // add the tuple to the proper bucket
        //   hashTuple->next.unshared = hashtable->buckets.unshared[bucketno];
        //   hashtable->buckets.unshared[bucketno] = hashTuple;
        let idx = HashTupleIdx(i);
        match &mut hashtable.buckets {
            HashJoinBuckets::Unshared(heads) => {
                let old_head = heads[bucketno as usize];
                hashtable.tuples[i].next = HashJoinTupleLink::Unshared(old_head);
                heads[bucketno as usize] = Some(idx);
            }
            HashJoinBuckets::Shared(_) => {
                unreachable!("ExecHashIncreaseNumBuckets with shared buckets (nodeHash.c:1587)")
            }
        }

        // CHECK_FOR_INTERRUPTS(): owned by the signal/interrupt subsystem; a
        // fake check would silently drop cancellation, so the C call site is
        // left as a documented note.
    }

    Ok(())
}

// ===========================================================================
//                           dense_alloc
// ===========================================================================

/// `dense_alloc(HashJoinTable hashtable, Size size)` (nodeHash.c:2896) —
/// allocate space for `size` bytes of tuple data within the dense-allocation
/// chunks; returns the staged tuple's arena index. Allocates in `mcx`.
///
/// OWNED-MODEL TRANSLATION: C returns a `char *` into the chunk's byte buffer
/// where the caller writes the `HashJoinTupleData` header + minimal tuple. The
/// owned model has no byte buffer — the tuple lives in the `tuples` arena — so
/// this performs the chunk byte-accounting (which drives `spaceUsed`) AND
/// reserves a fresh (placeholder) arena slot for the tuple, returning its
/// [`HashTupleIdx`]; the caller writes the real `hashvalue`/`mintuple`/`next`.
pub fn dense_alloc<'mcx>(
    mcx: Mcx<'mcx>,
    hashtable: &mut HashJoinTableData<'mcx>,
    size: Size,
) -> PgResult<HashTupleIdx> {
    // just in case the size is not already aligned properly
    let size = MAXALIGN(size);

    // Reserve the arena slot the caller will fill. Allocating fails as OOM.
    if hashtable.tuples.try_reserve(1).is_err() {
        return Err(mcx.oom(size));
    }
    let tuple_idx = HashTupleIdx(hashtable.tuples.len());
    hashtable.tuples.push(HashJoinTupleData {
        next: HashJoinTupleLink::Unshared(None),
        hashvalue: 0,
        mintuple: empty_mintuple(mcx)?,
    });

    // If tuple size is larger than threshold, allocate a separate chunk.
    if size > HASH_CHUNK_THRESHOLD {
        // allocate new chunk and put it at the beginning of the list
        if hashtable.chunk_arena.try_reserve(1).is_err() {
            return Err(mcx.oom(size));
        }
        let new_idx = HashChunkIdx(hashtable.chunk_arena.len());
        hashtable.chunk_arena.push(HashMemoryChunkData {
            ntuples: 1,
            maxlen: size,
            used: size,
            next: HashMemoryChunkLink::Unshared(None),
        });

        // Add this chunk to the list after the first existing chunk, so that we
        // don't lose the remaining space in the "current" chunk.
        if let Some(head) = hashtable.chunks {
            // newChunk->next = hashtable->chunks->next;
            let head_next = hashtable.chunk_arena[head.0].next;
            hashtable.chunk_arena[new_idx.0].next = head_next;
            // hashtable->chunks->next.unshared = newChunk;
            hashtable.chunk_arena[head.0].next = HashMemoryChunkLink::Unshared(Some(new_idx));
        } else {
            // newChunk->next.unshared = hashtable->chunks;  (NULL)
            hashtable.chunk_arena[new_idx.0].next = HashMemoryChunkLink::Unshared(None);
            hashtable.chunks = Some(new_idx);
        }
        return Ok(tuple_idx);
    }

    // See if we have enough space for it in the current chunk (if any). If not,
    // allocate a fresh chunk.
    let need_fresh = match hashtable.chunks {
        None => true,
        Some(head) => {
            let c = &hashtable.chunk_arena[head.0];
            (c.maxlen - c.used) < size
        }
    };

    if need_fresh {
        // allocate new chunk and put it at the beginning of the list
        if hashtable.chunk_arena.try_reserve(1).is_err() {
            return Err(mcx.oom(HASH_CHUNK_SIZE));
        }
        let new_idx = HashChunkIdx(hashtable.chunk_arena.len());
        let old_head = hashtable.chunks;
        hashtable.chunk_arena.push(HashMemoryChunkData {
            ntuples: 1,
            maxlen: HASH_CHUNK_SIZE,
            used: size,
            next: HashMemoryChunkLink::Unshared(old_head),
        });
        hashtable.chunks = Some(new_idx);
        return Ok(tuple_idx);
    }

    // There is enough space in the current chunk, let's add the tuple.
    let head = hashtable.chunks.expect("current chunk present");
    let c = &mut hashtable.chunk_arena[head.0];
    c.used += size;
    c.ntuples += 1;

    Ok(tuple_idx)
}

/// An empty `FormedMinimalTuple` placeholder for a freshly-reserved arena slot
/// (the caller overwrites it with the real tuple). Mirrors the uninitialized
/// dense-buffer bytes C hands back from `dense_alloc`.
#[inline]
fn empty_mintuple<'mcx>(mcx: Mcx<'mcx>) -> PgResult<FormedMinimalTuple<'mcx>> {
    Ok(FormedMinimalTuple {
        tuple: mcx::alloc_in(
            mcx,
            MinimalTupleData {
                t_len: 0,
                mt_padding: [0; 6],
                t_infomask2: 0,
                t_infomask: 0,
                t_hoff: 0,
                t_bits: PgVec::new_in(mcx),
            },
        )?,
        data: PgVec::new_in(mcx),
    })
}

// ===========================================================================
//                          ExecHashTableInsert
// ===========================================================================

/// `ExecHashTableInsert(HashJoinTable hashtable, TupleTableSlot *slot,
/// uint32 hashvalue)` (nodeHash.c:1749) — insert a tuple into the appropriate
/// bucket of the (serial) hash table, spilling to batch files past batch 0.
pub fn ExecHashTableInsert<'mcx>(
    mcx: Mcx<'mcx>,
    hashtable: &mut HashJoinTableData<'mcx>,
    estate: &mut types_nodes::EStateData<'mcx>,
    slot: types_nodes::SlotId,
    hashvalue: uint32,
) -> PgResult<()> {
    // bool shouldFree;
    // MinimalTuple tuple = ExecFetchSlotMinimalTuple(slot, &shouldFree);
    // ... insert tuple ...
    // if (shouldFree) heap_free_minimal_tuple(tuple);
    //
    // The execTuples seam copies the slot's tuple into mcx, so the owned model
    // never frees explicitly (the copy is dropped with the context).
    let (tuple, _should_free) =
        backend_executor_execTuples_seams::exec_fetch_slot_minimal_tuple::call(mcx, estate, slot)?;
    exec_hash_table_insert_tuple(mcx, hashtable, tuple, hashvalue)
}

/// `ExecHashTableInsert`'s tuple-half, parameterized on the already-fetched
/// owned `MinimalTuple` (the part that does not cross the execTuples seam).
/// Used by the body above once the slot fetch lands and reused by callers that
/// already hold a `MinimalTuple` (mirrors C's reuse note in
/// `ExecHashRemoveNextSkewBucket`). Implements 100% of the C insert logic.
pub fn exec_hash_table_insert_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    hashtable: &mut HashJoinTableData<'mcx>,
    tuple: FormedMinimalTuple<'mcx>,
    hashvalue: uint32,
) -> PgResult<()> {
    let bb = ExecHashGetBucketAndBatch(hashtable, hashvalue);
    let bucketno = bb.bucketno;
    let batchno = bb.batchno;

    // decide whether to put the tuple in the hash table or a temp file
    if batchno == hashtable.curbatch {
        // put the tuple in hash table
        let ntuples = hashtable.totalTuples - hashtable.skewTuples;

        // Create the HashJoinTuple.
        let hash_tuple_size = HJTUPLE_OVERHEAD + tuple.tuple.t_len as usize;
        let new_idx = dense_alloc(mcx, hashtable, hash_tuple_size)?;

        // hashTuple->hashvalue = hashvalue;
        // memcpy(HJTUPLE_MINTUPLE(hashTuple), tuple, tuple->t_len);
        let mut tuple = tuple;
        // We always reset the tuple-matched flag on insertion.
        //   HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(hashTuple));
        tuple.tuple.t_infomask2 &= !HEAP_TUPLE_HAS_MATCH;
        {
            let dst = &mut hashtable.tuples[new_idx.0];
            dst.hashvalue = hashvalue;
            dst.mintuple = tuple;
        }

        // Push it onto the front of the bucket's list.
        //   hashTuple->next.unshared = hashtable->buckets.unshared[bucketno];
        //   hashtable->buckets.unshared[bucketno] = hashTuple;
        match &mut hashtable.buckets {
            HashJoinBuckets::Unshared(heads) => {
                let old_head = heads[bucketno as usize];
                hashtable.tuples[new_idx.0].next = HashJoinTupleLink::Unshared(old_head);
                heads[bucketno as usize] = Some(new_idx);
            }
            HashJoinBuckets::Shared(_) => {
                unreachable!("serial ExecHashTableInsert with shared buckets (nodeHash.c:1749)")
            }
        }

        // Increase the (optimal) number of buckets if we just exceeded the
        // NTUP_PER_BUCKET threshold, but only when there's still a single batch.
        if hashtable.nbatch == 1
            && ntuples > (hashtable.nbuckets_optimal as f64 * NTUP_PER_BUCKET as f64)
        {
            // Guard against integer overflow and alloc size overflow.
            if hashtable.nbuckets_optimal <= INT_MAX / 2
                && (hashtable.nbuckets_optimal as usize) * 2
                    <= MaxAllocSize / SIZEOF_HASHJOINTUPLE_PTR
            {
                hashtable.nbuckets_optimal *= 2;
                hashtable.log2_nbuckets_optimal += 1;
            }
        }

        // Account for space used, and back off if we've used too much.
        hashtable.spaceUsed += hash_tuple_size;
        if hashtable.spaceUsed > hashtable.spacePeak {
            hashtable.spacePeak = hashtable.spaceUsed;
        }
        if hashtable.spaceUsed
            + (hashtable.nbuckets_optimal as usize) * SIZEOF_HASHJOINTUPLE_PTR
            > hashtable.spaceAllowed
        {
            ExecHashIncreaseNumBatches(mcx, hashtable)?;
        }
    } else {
        // put the tuple into a temp file for later batches
        debug_assert!(batchno > hashtable.curbatch);
        let blob = mintuple_to_flat(mcx, &tuple)?;
        let file = &mut hashtable.innerBatchFile[batchno as usize];
        backend_executor_nodeHashjoin_seams::ExecHashJoinSaveTuple::call(
            mcx, &blob, hashvalue, file,
        )?;
    }

    Ok(())
}

// ===========================================================================
//                       ExecHashGetBucketAndBatch
// ===========================================================================

/// `ExecHashGetBucketAndBatch(HashJoinTable hashtable, uint32 hashvalue,
/// int *bucketno, int *batchno)` (nodeHash.c:1960) — split a hash value into
/// its bucket and batch numbers. Pure arithmetic.
pub fn ExecHashGetBucketAndBatch<'mcx>(
    hashtable: &HashJoinTableData<'mcx>,
    hashvalue: uint32,
) -> BucketAndBatch {
    let nbuckets = hashtable.nbuckets as u32;
    let nbatch = hashtable.nbatch as u32;

    if nbatch > 1 {
        BucketAndBatch {
            bucketno: (hashvalue & (nbuckets - 1)) as i32,
            batchno: (pg_rotate_right32(hashvalue, hashtable.log2_nbuckets as u32)
                & (nbatch - 1)) as i32,
        }
    } else {
        BucketAndBatch {
            bucketno: (hashvalue & (nbuckets - 1)) as i32,
            batchno: 0,
        }
    }
}

// ===========================================================================
//                          ExecScanHashBucket
// ===========================================================================

/// `ExecScanHashBucket(HashJoinState *hjstate, ExprContext *econtext)`
/// (nodeHash.c:1992) — scan a (serial) hash bucket for matches to the current
/// outer tuple; returns `true` when a match was found.
pub fn ExecScanHashBucket<'mcx>(
    hjstate: &mut HashJoinState<'mcx>,
    estate: &mut types_nodes::EStateData<'mcx>,
) -> PgResult<bool> {
    let mcx = estate.es_query_cxt;
    let hashvalue = hjstate.hj_CurHashValue;
    let cur_bucket = hjstate.hj_CurBucketNo;
    let cur_skew = hjstate.hj_CurSkewBucketNo;
    let hash_tuple_slot = hjstate
        .hj_HashTupleSlot
        .expect("ExecScanHashBucket: hj_HashTupleSlot must be set up by init");
    let econtext_id = hjstate
        .js
        .ps
        .ps_ExprContext
        .expect("ExecScanHashBucket: ps_ExprContext");
    // hashclauses ExprState (the per-node compiled hash-join clause qual).
    let hashclauses = hjstate
        .hashclauses
        .as_deref_mut()
        .expect("ExecScanHashBucket: hashclauses must be compiled by init")
        as *mut types_nodes::execexpr::ExprState;

    // Helper: next-link of an arena tuple (serial mode only follows Unshared).
    fn tuple_next(ht: &HashJoinTableData<'_>, idx: HashTupleIdx) -> Option<HashTupleIdx> {
        match ht.tuples[idx.0].next {
            HashJoinTupleLink::Unshared(n) => n,
            HashJoinTupleLink::Shared(_) => None,
        }
    }

    let ht_ref = hjstate
        .hj_HashTable
        .as_ref()
        .expect("ExecScanHashBucket: hj_HashTable is NULL");

    // hj_CurTuple is the address of the tuple last returned from the current
    // bucket, or NULL if it's time to start scanning a new bucket. If the tuple
    // hashed to a skew bucket scan that, otherwise the standard bucket.
    let mut hash_tuple: Option<HashTupleIdx> = if let Some(cur) = hjstate.hj_CurTuple {
        tuple_next(ht_ref, cur)
    } else if cur_skew != INVALID_SKEW_BUCKET_NO {
        ht_ref.skewBucket[cur_skew as usize]
            .as_ref()
            .and_then(|b| b.tuples)
    } else {
        match &ht_ref.buckets {
            HashJoinBuckets::Unshared(b) => b[cur_bucket as usize],
            HashJoinBuckets::Shared(_) => None,
        }
    };

    while let Some(idx) = hash_tuple {
        // Read everything needed off the table in a tight scope so the
        // immutable borrow ends before we mutate hjstate/estate below.
        let (next, is_match, mtup_copy) = {
            let ht_ref = hjstate
                .hj_HashTable
                .as_ref()
                .expect("ExecScanHashBucket: hj_HashTable is NULL");
            let next = tuple_next(ht_ref, idx);
            let is_match = ht_ref.tuples[idx.0].hashvalue == hashvalue;
            let mtup_copy = if is_match {
                Some(ht_ref.tuples[idx.0].mintuple.clone_in(mcx)?)
            } else {
                None
            };
            (next, is_match, mtup_copy)
        };
        if is_match {
            // insert hashtable's tuple into exec slot so ExecQual sees it:
            //   inntuple = ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple),
            //                                    hjstate->hj_HashTupleSlot, false);
            //   econtext->ecxt_innertuple = inntuple;
            // The owned arena stores the MinimalTuple once; copy it into mcx and
            // force-store into the hash-tuple slot (the slot's ops own the copy).
            let mtup = mtup_copy.expect("is_match");
            backend_executor_execTuples_seams::exec_force_store_minimal_tuple::call(
                hash_tuple_slot,
                mtup,
                false,
                estate,
            )?;
            estate.ecxt_mut(econtext_id).ecxt_innertuple = Some(hash_tuple_slot);

            // if (ExecQualAndReset(hjclauses, econtext)) {
            //     hjstate->hj_CurTuple = hashTuple; return true; }
            let pass = backend_executor_execExpr_seams::exec_qual_and_reset::call(
                unsafe { &mut *hashclauses },
                econtext_id,
                estate,
            )?;
            if pass {
                hjstate.hj_CurTuple = Some(idx);
                return Ok(true);
            }
        }
        // hashTuple = hashTuple->next.unshared;
        hash_tuple = next;
    }

    // no match
    Ok(false)
}

// ===========================================================================
//                     ExecPrepHashTableForUnmatched
// ===========================================================================

/// `ExecPrepHashTableForUnmatched(HashJoinState *hjstate)` (nodeHash.c:2104) —
/// set up state to scan the (serial) hash table for unmatched inner tuples.
pub fn ExecPrepHashTableForUnmatched<'mcx>(hjstate: &mut HashJoinState<'mcx>) {
    // hj_CurBucketNo: next regular bucket to scan
    // hj_CurSkewBucketNo: next skew bucket (an index into skewBucketNums)
    // hj_CurTuple: last tuple returned, or NULL to start next bucket
    hjstate.hj_CurBucketNo = 0;
    hjstate.hj_CurSkewBucketNo = 0;
    hjstate.hj_CurTuple = None;
}

// ===========================================================================
//                    ExecScanHashTableForUnmatched
// ===========================================================================

/// `ExecScanHashTableForUnmatched(HashJoinState *hjstate, ExprContext *econtext)`
/// (nodeHash.c:2190) — return the next unmatched inner tuple (serial path).
pub fn ExecScanHashTableForUnmatched<'mcx>(
    hjstate: &mut HashJoinState<'mcx>,
    estate: &mut types_nodes::EStateData<'mcx>,
) -> PgResult<bool> {
    let mcx = estate.es_query_cxt;
    let hash_tuple_slot = hjstate
        .hj_HashTupleSlot
        .expect("ExecScanHashTableForUnmatched: hj_HashTupleSlot must be set up by init");
    let econtext_id = hjstate
        .js
        .ps
        .ps_ExprContext
        .expect("ExecScanHashTableForUnmatched: ps_ExprContext");
    // We mutate hjstate cursor fields while reading the (immutable) hashtable;
    // the chain walk is read-only; the match-flag test is pure owned-data logic.
    let mut hash_tuple: Option<HashTupleIdx> = hjstate.hj_CurTuple;

    loop {
        // hj_CurTuple is the address of the tuple last returned from the current
        // bucket, or NULL if it's time to start scanning a new bucket.
        {
            let hashtable = hjstate
                .hj_HashTable
                .as_ref()
                .expect("ExecScanHashTableForUnmatched: hj_HashTable is NULL");
            if let Some(cur) = hash_tuple {
                // hashTuple = hashTuple->next.unshared;
                hash_tuple = match hashtable.tuples[cur.0].next {
                    HashJoinTupleLink::Unshared(n) => n,
                    HashJoinTupleLink::Shared(_) => None,
                };
            } else if hjstate.hj_CurBucketNo < hashtable.nbuckets {
                // hashTuple = hashtable->buckets.unshared[hj_CurBucketNo];
                hash_tuple = match &hashtable.buckets {
                    HashJoinBuckets::Unshared(b) => b[hjstate.hj_CurBucketNo as usize],
                    HashJoinBuckets::Shared(_) => None,
                };
                hjstate.hj_CurBucketNo += 1;
            } else if hjstate.hj_CurSkewBucketNo < hashtable.nSkewBuckets {
                // int j = hashtable->skewBucketNums[hj_CurSkewBucketNo];
                // hashTuple = hashtable->skewBucket[j]->tuples;
                let j = hashtable.skewBucketNums[hjstate.hj_CurSkewBucketNo as usize] as usize;
                hash_tuple = hashtable.skewBucket[j].as_ref().and_then(|b| b.tuples);
                hjstate.hj_CurSkewBucketNo += 1;
            } else {
                break; // finished all buckets
            }
        }

        while let Some(idx) = hash_tuple {
            let (has_match, next, mtup_copy) = {
                let hashtable = hjstate.hj_HashTable.as_ref().unwrap();
                let has_match =
                    hashtable.tuples[idx.0].mintuple.tuple.t_infomask2 & HEAP_TUPLE_HAS_MATCH != 0;
                let next = match hashtable.tuples[idx.0].next {
                    HashJoinTupleLink::Unshared(n) => n,
                    HashJoinTupleLink::Shared(_) => None,
                };
                let mtup_copy = if !has_match {
                    Some(hashtable.tuples[idx.0].mintuple.clone_in(mcx)?)
                } else {
                    None
                };
                (has_match, next, mtup_copy)
            };
            if !has_match {
                // insert hashtable's tuple into exec slot:
                //   inntuple = ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple),
                //                                    hj_HashTupleSlot, false);
                //   econtext->ecxt_innertuple = inntuple;
                //   ResetExprContext(econtext);
                //   hjstate->hj_CurTuple = hashTuple; return true;
                let mtup = mtup_copy.expect("!has_match");
                backend_executor_execTuples_seams::exec_force_store_minimal_tuple::call(
                    hash_tuple_slot,
                    mtup,
                    false,
                    estate,
                )?;
                estate.ecxt_mut(econtext_id).ecxt_innertuple = Some(hash_tuple_slot);
                // Reset temp memory each time (keeps parallel to ExecScanHashBucket).
                backend_executor_execUtils_seams::reset_per_tuple_expr_context::call(
                    estate,
                    &hjstate.js.ps,
                )?;
                hjstate.hj_CurTuple = Some(idx);
                return Ok(true);
            }

            // hashTuple = hashTuple->next.unshared;
            hash_tuple = next;
        }

        // CHECK_FOR_INTERRUPTS(): owned by the signal/interrupt subsystem; a
        // fake check would silently drop cancellation, so the C site is a note.
    }

    // no more unmatched tuples
    Ok(false)
}

// ===========================================================================
//                          ExecHashTableReset
// ===========================================================================

/// `ExecHashTableReset(HashJoinTable hashtable)` (nodeHash.c:2327) — reset the
/// hashtable for a new batch (reset `batchCxt`, zero the bucket array).
pub fn ExecHashTableReset<'mcx>(
    mcx: Mcx<'mcx>,
    hashtable: &mut HashJoinTableData<'mcx>,
) -> PgResult<()> {
    let nbuckets = hashtable.nbuckets;

    // Release all the hash buckets and tuples acquired in the prior pass (the
    // batchCxt reset frees the dense chunks + tuples), and reinitialize for a
    // new pass. The owned model clears the per-batch arenas.
    hashtable.tuples.clear();

    // Reallocate and reinitialize the hash bucket headers.
    //   hashtable->buckets.unshared = palloc0_array(HashJoinTuple, nbuckets);
    let buckets = alloc_empty_bucket_indices(mcx, nbuckets)?;
    hashtable.buckets = HashJoinBuckets::Unshared(buckets);

    hashtable.spaceUsed = 0;

    // Forget the chunks (the memory was freed by the context reset above).
    hashtable.chunk_arena.clear();
    hashtable.chunks = None;
    hashtable.current_chunk = None;

    Ok(())
}

// ===========================================================================
//                      ExecHashTableResetMatchFlags
// ===========================================================================

/// `ExecHashTableResetMatchFlags(HashJoinTable hashtable)` (nodeHash.c:2355) —
/// clear the `HEAP_TUPLE_HAS_MATCH` flag on every in-memory and skew tuple
/// (for right/full joins rescanned per batch).
pub fn ExecHashTableResetMatchFlags<'mcx>(hashtable: &mut HashJoinTableData<'mcx>) {
    // Reset all flags in the main table ...
    for i in 0..hashtable.nbuckets as usize {
        let mut t = match &hashtable.buckets {
            HashJoinBuckets::Unshared(b) => b[i],
            HashJoinBuckets::Shared(_) => None,
        };
        while let Some(idx) = t {
            // HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(tuple));
            hashtable.tuples[idx.0].mintuple.tuple.t_infomask2 &= !HEAP_TUPLE_HAS_MATCH;
            t = match hashtable.tuples[idx.0].next {
                HashJoinTupleLink::Unshared(n) => n,
                HashJoinTupleLink::Shared(_) => None,
            };
        }
    }

    // ... and the same for the skew buckets, if any.
    for i in 0..hashtable.nSkewBuckets as usize {
        let j = hashtable.skewBucketNums[i] as usize;
        let mut t = hashtable.skewBucket[j].as_ref().and_then(|b| b.tuples);
        while let Some(idx) = t {
            hashtable.tuples[idx.0].mintuple.tuple.t_infomask2 &= !HEAP_TUPLE_HAS_MATCH;
            t = match hashtable.tuples[idx.0].next {
                HashJoinTupleLink::Unshared(n) => n,
                HashJoinTupleLink::Shared(_) => None,
            };
        }
    }
}

// ===========================================================================
//                        get_hash_memory_limit
// ===========================================================================

/// `get_hash_memory_limit(void)` (nodeHash.c:3622) — the per-hash memory budget
/// in bytes (`work_mem * hash_mem_multiplier`, capped at `SIZE_MAX`). Reads the
/// backend GUCs; passed explicitly until the GUC owner lands.
pub fn get_hash_memory_limit(work_mem: i32, hash_mem_multiplier: f64) -> Size {
    // Do initial calculation in double arithmetic.
    let mut mem_limit = work_mem as f64 * hash_mem_multiplier * 1024.0;

    // Clamp in case it doesn't fit in size_t.
    mem_limit = mem_limit.min(SIZE_MAX as f64);

    mem_limit as Size
}

/// The default `get_hash_memory_limit()` value used by the EState-less
/// `ExecChooseHashTableSize` entry point. The two memory GUCs (`work_mem`,
/// `hash_mem_multiplier`) are backend globals owned by the GUC subsystem, which
/// the sizing signature does not thread; reading them here is the seam-boundary
/// panic until the GUC owner lands (mirrors the `get_hash_memory_limit`
/// `work_mem * hash_mem_multiplier * 1024` formula once the values are
/// reachable).
fn get_hash_memory_limit_fixed() -> Size {
    // get_hash_memory_limit() reads work_mem / hash_mem_multiplier as backend
    // GUC globals; the parallel module carries them as a backend thread_local
    // (per AGENTS.md) until the GUC owner installs a setter.
    let (work_mem, hash_mem_multiplier) = crate::parallel::hash_mem_gucs();
    get_hash_memory_limit(work_mem, hash_mem_multiplier)
}
