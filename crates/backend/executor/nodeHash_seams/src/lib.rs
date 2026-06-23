//! Seam declarations for the `backend-executor-nodeHash` unit
//! (`executor/nodeHash.c`): the hash-table library the hash join drives.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. The owned model threads `&mut HashJoinState`
//! (which owns `hj_HashTable`) and `&mut EStateData` explicitly in place of the
//! C `HashJoinTable` / `ExprContext` / `PlanState.state` pointers.

#![allow(non_snake_case)]

use types_core::primitive::Oid;
use types_error::PgResult;
use ::nodes::primnodes::Expr;
use ::nodes::nodehashjoin::{ExprState, HashJoinState};
use ::nodes::EStateData;

seam_core::seam!(
    /// `ExecHashTableCreate(node)` (nodeHash.c): build the empty hash table for
    /// the inner relation and install it on the node (`node->hj_HashTable` and
    /// the inner HashState's `hashtable`). Allocates in the per-query context.
    pub fn exec_hash_table_create<'mcx>(
        node: &mut HashJoinState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `MultiExecProcNode((PlanState *) hashNode)` (execProcnode.c → nodeHash.c
    /// `MultiExecHash`): execute the inner Hash node to populate the hash table.
    pub fn multi_exec_hash<'mcx>(
        node: &mut HashJoinState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecHashTableDestroy(node->hj_HashTable)` (nodeHash.c): free the hash
    /// table and clear `node->hj_HashTable` to `None`.
    pub fn exec_hash_table_destroy<'mcx>(node: &mut HashJoinState<'mcx>) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecHashTableReset(hashtable)` (nodeHash.c): reset the hash table for a
    /// new batch (`MemoryContextReset(batchCxt)` + re-zero the bucket array).
    pub fn exec_hash_table_reset<'mcx>(node: &mut HashJoinState<'mcx>) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecHashTableResetMatchFlags(hashtable)` (nodeHash.c): clear the match
    /// flag on every tuple in the (single-batch) hash table for a rescan.
    pub fn exec_hash_table_reset_match_flags<'mcx>(node: &mut HashJoinState<'mcx>) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecHashTableInsert(hashtable, slot, hashvalue)` (nodeHash.c): insert a
    /// tuple into the hash table (it may be sent to a future batch and may
    /// trigger an nbatch increase). The slot is identified by its EState id.
    pub fn exec_hash_table_insert<'mcx>(
        node: &mut HashJoinState<'mcx>,
        slot: ::nodes::SlotId,
        hashvalue: u32,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecHashGetBucketAndBatch(hashtable, hashvalue, &bucketno, &batchno)`
    /// (nodeHash.c): derive the bucket and batch number for a hash value. The
    /// bucket number is written onto `node.hj_CurBucketNo`; the batch number is
    /// returned. Infallible at the ereport level (pure arithmetic).
    pub fn exec_hash_get_bucket_and_batch<'mcx>(
        node: &mut HashJoinState<'mcx>,
        hashvalue: u32,
    ) -> i32
);

seam_core::seam!(
    /// `ExecHashGetSkewBucket(hashtable, hashvalue)` (nodeHash.c): the skew
    /// bucket number for a hash value, or `INVALID_SKEW_BUCKET_NO`.
    pub fn exec_hash_get_skew_bucket<'mcx>(node: &HashJoinState<'mcx>, hashvalue: u32) -> i32
);

seam_core::seam!(
    /// `ExecScanHashBucket(hjstate, econtext)` (nodeHash.c): advance to the next
    /// tuple in the current bucket that matches the hash clauses, storing it as
    /// `econtext->ecxt_innertuple` and advancing `hj_CurTuple`. Returns true if
    /// a candidate was found. Fallible (runs the hash-clause ExprState).
    pub fn exec_scan_hash_bucket<'mcx>(
        node: &mut HashJoinState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `ExecPrepHashTableForUnmatched(hjstate)` (nodeHash.c): set the
    /// unmatched-scan cursors to the start state.
    pub fn exec_prep_hash_table_for_unmatched<'mcx>(node: &mut HashJoinState<'mcx>) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecScanHashTableForUnmatched(hjstate, econtext)` (nodeHash.c): return
    /// the next inner tuple whose match flag is clear (right/full joins),
    /// storing it as `econtext->ecxt_innertuple`. Returns true while any remain.
    pub fn exec_scan_hash_table_for_unmatched<'mcx>(
        node: &mut HashJoinState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `HeapTupleHeaderHasMatch(HJTUPLE_MINTUPLE(hj_CurTuple))` — whether the
    /// current bucket tuple's match flag is set.
    pub fn cur_tuple_has_match<'mcx>(node: &HashJoinState<'mcx>) -> bool
);

seam_core::seam!(
    /// `HeapTupleHeaderSetMatch(HJTUPLE_MINTUPLE(hj_CurTuple))` — set the
    /// current bucket tuple's match flag.
    pub fn cur_tuple_set_match<'mcx>(node: &mut HashJoinState<'mcx>) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecHashAccumInstrumentation(hinstrument, hashtable)` (nodeHash.c):
    /// accumulate stats from the (about-to-be-destroyed) hash table into the
    /// inner Hash node's instrumentation, allocating it first if needed (the C
    /// `ExecReScanHashJoin` rebuild path).
    pub fn exec_hash_accum_instrumentation<'mcx>(node: &mut HashJoinState<'mcx>) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecBuildHash32Expr(...)` (nodeHash.c): compile the per-side hash-value
    /// ExprState. `is_outer` selects which side (outer → `hj_OuterHash`, inner →
    /// the HashState's `hash_expr`); `keep_nulls` is the `HJ_FILL_*` flag.
    /// `hashfuncids`/`collations` are the per-key resolved hash functions and
    /// collations; `hash_exprs` are the per-side hash key expressions
    /// (`node->hashkeys` for outer, `hash->hashkeys` for inner) and `opstrict`
    /// the per-key `op_strict(hashop)` flags. The node's result `desc`/`ops` are
    /// read off the relevant PlanState (`js.ps` for outer, the inner HashState's
    /// `ps` for inner). The actual compilation is delegated to execExpr's
    /// `ExecBuildHash32Expr`; nodeHash owns this seam only because the inner
    /// program is stored on the inner `HashState`'s `hash_expr`. Returns the
    /// compiled ExprState (outer side; inner side is also stored on the
    /// HashState). Allocates.
    pub fn exec_build_hash32_expr<'mcx>(
        node: &mut HashJoinState<'mcx>,
        is_outer: bool,
        hashfuncids: &[Oid],
        collations: &[Oid],
        hash_exprs: &[Expr<'mcx>],
        opstrict: &[bool],
        keep_nulls: bool,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<mcx::PgBox<'mcx, ExprState<'mcx>>>
);

seam_core::seam!(
    /// Set up the inner HashState's skew hash function from the first key's hash
    /// function OID (nodeHashjoin.c `if (OidIsValid(hash->skewTable))` block:
    /// `palloc0(FmgrInfo)`, `skew_collation`, `fmgr_info(outer_hashfuncid[0])`).
    /// Owned by nodeHash since it writes the HashState. `skew_table_valid` is
    /// `OidIsValid(hash->skewTable)`; a no-op when false.
    pub fn setup_skew_hashfunction<'mcx>(
        node: &mut HashJoinState<'mcx>,
        skew_hashfuncid: Oid,
        skew_collation: Oid,
        skew_table_valid: bool,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

// ===========================================================================
// Parallel hash table (nodeHash.c, parallel-aware paths).
// ===========================================================================

seam_core::seam!(
    /// `ExecParallelScanHashBucket(hjstate, econtext)` (nodeHash.c): the
    /// parallel-aware bucket scan over the DSA-resident hash table.
    pub fn exec_parallel_scan_hash_bucket<'mcx>(
        node: &mut HashJoinState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `ExecParallelScanHashTableForUnmatched(hjstate, econtext)` (nodeHash.c).
    pub fn exec_parallel_scan_hash_table_for_unmatched<'mcx>(
        node: &mut HashJoinState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `ExecParallelPrepHashTableForUnmatched(hjstate)` (nodeHash.c): elect one
    /// backend per batch to do the unmatched scan. Returns true if this backend
    /// is the chosen one.
    pub fn exec_parallel_prep_hash_table_for_unmatched<'mcx>(
        node: &mut HashJoinState<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `ExecParallelHashTableAlloc(hashtable, batchno)` (nodeHash.c): allocate
    /// the DSA-resident bucket array for a batch.
    pub fn exec_parallel_hash_table_alloc<'mcx>(
        node: &mut HashJoinState<'mcx>,
        batchno: i32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecParallelHashTableSetCurrentBatch(hashtable, batchno)` (nodeHash.c).
    pub fn exec_parallel_hash_table_set_current_batch<'mcx>(
        node: &mut HashJoinState<'mcx>,
        batchno: i32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecParallelHashTableInsertCurrentBatch(hashtable, slot, hashvalue)`
    /// (nodeHash.c).
    pub fn exec_parallel_hash_table_insert_current_batch<'mcx>(
        node: &mut HashJoinState<'mcx>,
        slot: ::nodes::SlotId,
        hashvalue: u32,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecHashTableDetach(hashtable)` (nodeHash.c): detach from the
    /// DSM-resident build barrier.
    pub fn exec_hash_table_detach<'mcx>(node: &mut HashJoinState<'mcx>) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecHashTableDetachBatch(hashtable)` (nodeHash.c): detach from the
    /// current batch's barrier, freeing per-batch shared memory if last.
    pub fn exec_hash_table_detach_batch<'mcx>(node: &mut HashJoinState<'mcx>) -> PgResult<()>
);

seam_core::seam!(
    /// Store a `MinimalTuple` produced by a parallel batch scan into the hash
    /// join's owned slot (`ExecForceStoreMinimalTuple(tuple, hj_*Slot, false)`),
    /// returning the slot id. The which-slot choice mirrors the call site
    /// (outer vs hash tuple slot).
    pub fn force_store_minimal_into_slot<'mcx>(
        slot: ::nodes::SlotId,
        tuple: types_tuple::heaptuple::FormedMinimalTuple<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `hashtable->parallel_state->build_barrier` phase, exposed for the
    /// parallel build handshake (`BarrierPhase(build_barrier)`).
    pub fn build_barrier_phase<'mcx>(node: &HashJoinState<'mcx>) -> i32
);

seam_core::seam!(
    /// `BarrierArriveAndWait(&parallel_state->build_barrier, wait_event)`
    /// (storage/ipc/barrier.c via nodeHash's shared state): arrive at the build
    /// barrier and wait. Returns true if this backend was elected (the C return
    /// value). `wait_event` is the `WAIT_EVENT_*` code.
    pub fn build_barrier_arrive_and_wait<'mcx>(
        node: &mut HashJoinState<'mcx>,
        wait_event: u32,
    ) -> PgResult<bool>
);

// --- Per-batch barrier + distributor (parallel-aware new-batch loop) ---
// These touch the nodeHash-owned shared per-batch state
// (`batches[batchno].shared->batch_barrier`) and the shared distributor, so
// they are nodeHash seams keyed by `batchno`.

seam_core::seam!(
    /// `pg_atomic_fetch_add_u32(&parallel_state->distributor, 1) % nbatch`
    /// (nodeHashjoin.c): pick the starting batch for this backend's search.
    pub fn parallel_distributor_next_start<'mcx>(node: &HashJoinState<'mcx>) -> u32
);

seam_core::seam!(
    /// `!hashtable->batches[batchno].done` — whether this batch still needs
    /// work.
    pub fn parallel_batch_not_done<'mcx>(node: &HashJoinState<'mcx>, batchno: i32) -> bool
);

seam_core::seam!(
    /// `BarrierAttach(&batches[batchno].shared->batch_barrier)`: attach and
    /// return the current `PHJ_BATCH_*` phase.
    pub fn parallel_batch_attach<'mcx>(node: &mut HashJoinState<'mcx>, batchno: i32) -> i32
);

seam_core::seam!(
    /// `BarrierArriveAndWait(&batches[batchno].shared->batch_barrier,
    /// wait_event)`: returns true if this backend was elected.
    pub fn parallel_batch_arrive_and_wait<'mcx>(
        node: &mut HashJoinState<'mcx>,
        batchno: i32,
        wait_event: u32,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `BarrierDetach(&batches[batchno].shared->batch_barrier)`.
    pub fn parallel_batch_detach<'mcx>(node: &mut HashJoinState<'mcx>, batchno: i32) -> PgResult<()>
);

seam_core::seam!(
    /// `BarrierPhase(&batches[batchno].shared->batch_barrier)` — for the
    /// `elog(ERROR, "unexpected batch phase %d")` diagnostic.
    pub fn parallel_batch_phase<'mcx>(node: &HashJoinState<'mcx>, batchno: i32) -> i32
);

seam_core::seam!(
    /// `hashtable->batches[batchno].done = true`.
    pub fn parallel_batch_set_done<'mcx>(node: &mut HashJoinState<'mcx>, batchno: i32) -> PgResult<()>
);

seam_core::seam!(
    /// `hashtable->curbatch = -1`.
    pub fn parallel_set_curbatch_invalid<'mcx>(node: &mut HashJoinState<'mcx>) -> PgResult<()>
);

seam_core::seam!(
    /// `hashtable->curbatch >= 0` — were we attached to a batch?
    pub fn parallel_has_curbatch<'mcx>(node: &HashJoinState<'mcx>) -> bool
);

seam_core::seam!(
    /// `get_hash_memory_limit()` (nodeHash.c) — the per-hash-operation working
    /// memory budget in bytes (`work_mem`/`hash_mem_multiplier`, clamped to
    /// `SIZE_MAX`). Consumed by Memoize to bound its cache. Infallible in
    /// practice; `PgResult` mirrors the C `ereport`-capable surface.
    pub fn get_hash_memory_limit() -> PgResult<u64>
);
