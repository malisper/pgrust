//! `nodeHash.c` — routines to hash relations for hashjoin (PostgreSQL 18.3).
//!
//! SCAFFOLD: every function has its real, C-faithful signature and a
//! decomp-placeholder stub body. Bodies are filled in per family module by later
//! agents.
//!
//! Function families (one module per family, so bodies fill in parallel):
//! - [`exec_hash`]    — the Hash executor-node lifecycle and the `MultiExec`
//!   build entry points (`ExecHash`, `MultiExecHash`, `MultiExecPrivateHash`,
//!   `MultiExecParallelHash`, `ExecInitHash`, `ExecEndHash`, `ExecReScanHash`,
//!   `ExecShutdownHash`).
//! - [`hash_table`]  — the serial in-memory hash table: create / size /
//!   build / probe / grow / reset plus the dense-allocator
//!   (`ExecHashTableCreate`, `ExecChooseHashTableSize`,
//!   `ExecHashTableDestroy`, `ExecHashIncreaseBatchSize`,
//!   `ExecHashIncreaseNumBatches`, `ExecHashIncreaseNumBuckets`,
//!   `ExecHashTableInsert`, `ExecHashGetBucketAndBatch`, `ExecScanHashBucket`,
//!   `ExecPrepHashTableForUnmatched`, `ExecScanHashTableForUnmatched`,
//!   `ExecHashTableReset`, `ExecHashTableResetMatchFlags`, `dense_alloc`,
//!   `get_hash_memory_limit`).
//! - [`skew`]        — the skew-optimization hashtable (`ExecHashBuildSkewHash`,
//!   `ExecHashGetSkewBucket`, `ExecHashSkewTableInsert`,
//!   `ExecHashRemoveNextSkewBucket`).
//! - [`parallel`]    — the Parallel Hash Join shared-memory machinery (the
//!   ~23 `ExecParallelHash*` / detach routines).
//! - [`instrument`]  — instrumentation and the parallel-DSM node hooks
//!   (`ExecHashEstimate`, `ExecHashInitializeDSM`, `ExecHashInitializeWorker`,
//!   `ExecHashRetrieveInstrumentation`, `ExecHashAccumInstrumentation`).
//!
//! Operations below the executor-node layer go through the owners' seam
//! crates: child dispatch (execProcnode), expr eval (execExpr), slot/econtext
//! ops (execTuples/execUtils), instrumentation (instrument), the skew-hash
//! catalog/fmgr lookups (syscache/lsyscache/fmgr), the tuple-spill callback
//! (nodeHashjoin), and the whole DSM/parallel stack (dsa / lwlock / barrier /
//! buffile / shared-tuplestore).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::result_large_err)]

pub mod exec_hash;
pub mod hash_table;
pub mod instrument;
pub mod parallel;
pub mod skew;

use types_core::Size;

// ===========================================================================
//                          Constants & macros
// ===========================================================================

/// `MAXIMUM_ALIGNOF` — 8 on 64-bit PostgreSQL.
pub(crate) const MAXIMUM_ALIGNOF: usize = 8;

/// `MAXALIGN(LEN)` (c.h) — round `len` up to `MAXIMUM_ALIGNOF`.
#[inline]
pub(crate) const fn MAXALIGN(len: usize) -> usize {
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// `MaxAllocSize` (memutils.h) — `0x3fffffff` (1 GB - 1).
pub(crate) const MaxAllocSize: usize = 0x3fff_ffff;

/// `SizeofMinimalTupleHeader` (htup_details.h) — `offsetof(MinimalTupleData,
/// t_bits)` = 23 on 64-bit PostgreSQL.
pub(crate) const SizeofMinimalTupleHeader: usize = 23;

/// `BLCKSZ` (pg_config.h) — default block size, 8192.
pub(crate) const BLCKSZ: usize = 8192;

/// `HJTUPLE_OVERHEAD` (hashjoin.h) — `MAXALIGN(sizeof(HashJoinTupleData))`.
/// In the owned model the per-tuple header is `next` (8) + `hashvalue` (4),
/// MAXALIGNed to 16; restated as a constant for the byte-accounting that
/// mirrors C's pointer arithmetic.
pub(crate) const HJTUPLE_OVERHEAD: usize = MAXALIGN(8 + 4);

/// `HASH_CHUNK_HEADER_SIZE` (hashjoin.h) — `MAXALIGN(sizeof(HashMemoryChunkData))`.
pub(crate) const HASH_CHUNK_HEADER_SIZE: usize = MAXALIGN(8 * 3 + 8);

/// `SKEW_BUCKET_OVERHEAD` (hashjoin.h) — `MAXALIGN(sizeof(HashSkewBucket))`.
pub(crate) const SKEW_BUCKET_OVERHEAD: usize = MAXALIGN(4 + 8);

// Re-export the hashjoin vocabulary the bodies and callers use.
pub use types_nodes::nodehash::{
    BucketAndBatch, HASH_CHUNK_SIZE, HASH_CHUNK_THRESHOLD, INVALID_SKEW_BUCKET_NO,
    SKEW_HASH_MEM_PERCENT, SKEW_MIN_OUTER_FRACTION,
};

/// Crates reached only through `panic!`-guarded seam-boundary paths (no
/// installed call site yet); kept as explicit `use` so the dependency edge is
/// recorded.
#[allow(unused_imports)]
mod _seam_deps {
    use backend_executor_instrument_seams as _instrument;
    use backend_utils_cache_syscache_seams as _syscache;
}

// ===========================================================================
//                        Seam adapters + installation
// ===========================================================================

/// Thin adapter free functions matching the `backend-executor-nodeHash-seams`
/// signatures exactly. Each resolves `node.hj_HashTable` to the
/// `HashJoinTableData`, resolves slot/econtext ids through the `EState`, calls
/// the C-faithful body in `hash_table` / `skew` / `parallel` / `instrument`,
/// and returns the declared `PgResult`. The owner installs all 34 from
/// [`init_seams`].
mod adapters {
    use backend_executor_execExpr_seams as execExpr;
    use mcx::PgBox;
    use types_error::{PgError, PgResult};
    use types_nodes::execnodes::{EStateData, SlotId};
    use types_nodes::execexpr::ExprState;
    use types_nodes::planstate::PlanStateNode;
    use types_nodes::nodehash::{
        HashJoinState, HashJoinTableData, HashJoinTupleLink, HashState,
    };
    use types_tuple::heaptuple::HEAP_TUPLE_HAS_MATCH;

    use crate::{hash_table, instrument, parallel, skew};

    /// `&mut node->hj_HashTable` (the C unconditional deref on these paths).
    #[inline]
    fn ht<'a, 'mcx>(node: &'a mut HashJoinState<'mcx>) -> &'a mut HashJoinTableData<'mcx> {
        node.hj_HashTable
            .as_deref_mut()
            .expect("nodeHash seam: hj_HashTable is NULL")
    }

    /// `&node->hj_HashTable`.
    #[inline]
    fn ht_ref<'a, 'mcx>(node: &'a HashJoinState<'mcx>) -> &'a HashJoinTableData<'mcx> {
        node.hj_HashTable
            .as_deref()
            .expect("nodeHash seam: hj_HashTable is NULL")
    }

    /// `(HashState *) innerPlanState(node)` — the inner Hash node's state,
    /// reached as `node->js.ps.righttree` (`PlanStateNode::Hash`).
    #[inline]
    fn inner_hash_state<'a, 'mcx>(node: &'a mut HashJoinState<'mcx>) -> &'a mut HashState<'mcx> {
        match node.js.ps.righttree.as_deref_mut() {
            Some(PlanStateNode::Hash(h)) => h,
            _ => panic!("nodeHash seam: innerPlanState(node) is not a HashState"),
        }
    }

    // ---- table lifecycle -------------------------------------------------

    pub fn exec_hash_table_create<'mcx>(
        node: &mut HashJoinState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()> {
        let mcx = estate.es_query_cxt;
        // ExecHashTableCreate reads the inner Hash plan node's
        // rows/width/skewTable through the inner HashState; build the table,
        // then install it on the join node and the inner HashState.
        let table = {
            let state = inner_hash_state(node);
            hash_table::ExecHashTableCreate(mcx, state)?
        };
        // node->hj_HashTable = hashtable; hashNode->hashtable = hashtable;
        // (The two C pointers alias one table; the owned model installs the
        // built table on the join node and lets the inner HashState reach it
        // through the join node, mirroring the single allocation.)
        node.hj_HashTable = Some(table);
        Ok(())
    }

    pub fn multi_exec_hash<'mcx>(
        node: &mut HashJoinState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()> {
        let mcx = estate.es_query_cxt;
        // MultiExecProcNode((PlanState *) hashNode) → MultiExecHash(hashNode).
        // The inner HashState must see the table the join node built; move it
        // in, run the build, then move it back onto the join node.
        let table = node
            .hj_HashTable
            .take()
            .expect("multi_exec_hash: hj_HashTable must be built first");
        {
            let state = inner_hash_state(node);
            state.hashtable = Some(table);
            exec_hash::MultiExecHash(mcx, state, estate)?;
        }
        let table = inner_hash_state(node).hashtable.take();
        node.hj_HashTable = table;
        Ok(())
    }
    use crate::exec_hash;

    pub fn exec_hash_table_destroy<'mcx>(node: &mut HashJoinState<'mcx>) -> PgResult<()> {
        if let Some(table) = node.hj_HashTable.take() {
            hash_table::ExecHashTableDestroy(table)?;
        }
        // C also clears the inner HashState's hashtable pointer.
        Ok(())
    }

    pub fn exec_hash_table_reset<'mcx>(node: &mut HashJoinState<'mcx>) -> PgResult<()> {
        let mcx = node.hj_HashTable.as_ref().map(|_| ()).map(|_| ());
        let _ = mcx;
        // ExecHashTableReset wants the query mcx; reach it through the table's
        // spillCxt (a child of the per-query context).
        let spill = ht(node).spillCxt;
        hash_table::ExecHashTableReset(spill, ht(node))
    }

    pub fn exec_hash_table_reset_match_flags<'mcx>(node: &mut HashJoinState<'mcx>) -> PgResult<()> {
        hash_table::ExecHashTableResetMatchFlags(ht(node));
        Ok(())
    }

    pub fn exec_hash_table_insert<'mcx>(
        node: &mut HashJoinState<'mcx>,
        slot: SlotId,
        hashvalue: u32,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()> {
        let mcx = estate.es_query_cxt;
        // ExecHashTableInsert(hashtable, slot, hashvalue). Split the borrows by
        // resolving the slot, then taking the table off the node for the call.
        let mut table = node
            .hj_HashTable
            .take()
            .expect("exec_hash_table_insert: hj_HashTable is NULL");
        let res = hash_table::ExecHashTableInsert(mcx, &mut table, estate, slot, hashvalue);
        node.hj_HashTable = Some(table);
        res
    }

    pub fn exec_hash_get_bucket_and_batch<'mcx>(
        node: &mut HashJoinState<'mcx>,
        hashvalue: u32,
    ) -> i32 {
        let bb = hash_table::ExecHashGetBucketAndBatch(ht_ref(node), hashvalue);
        // C writes *bucketno; the seam stores it on hj_CurBucketNo and returns
        // batchno.
        node.hj_CurBucketNo = bb.bucketno;
        bb.batchno
    }

    pub fn exec_hash_get_skew_bucket<'mcx>(node: &HashJoinState<'mcx>, hashvalue: u32) -> i32 {
        skew::ExecHashGetSkewBucket(ht_ref(node), hashvalue)
    }

    pub fn exec_scan_hash_bucket<'mcx>(
        node: &mut HashJoinState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<bool> {
        hash_table::ExecScanHashBucket(node, estate)
    }

    pub fn exec_prep_hash_table_for_unmatched<'mcx>(
        node: &mut HashJoinState<'mcx>,
    ) -> PgResult<()> {
        hash_table::ExecPrepHashTableForUnmatched(node);
        Ok(())
    }

    pub fn exec_scan_hash_table_for_unmatched<'mcx>(
        node: &mut HashJoinState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<bool> {
        hash_table::ExecScanHashTableForUnmatched(node, estate)
    }

    pub fn cur_tuple_has_match<'mcx>(node: &HashJoinState<'mcx>) -> bool {
        // HeapTupleHeaderHasMatch(HJTUPLE_MINTUPLE(node->hj_CurTuple)).
        let table = ht_ref(node);
        match node.hj_CurTuple {
            Some(idx) => table.tuples[idx.0].mintuple.tuple.t_infomask2 & HEAP_TUPLE_HAS_MATCH != 0,
            None => false,
        }
    }

    pub fn cur_tuple_set_match<'mcx>(node: &mut HashJoinState<'mcx>) -> PgResult<()> {
        // HeapTupleHeaderSetMatch(HJTUPLE_MINTUPLE(node->hj_CurTuple)).
        let cur = node.hj_CurTuple;
        let table = ht(node);
        if let Some(idx) = cur {
            table.tuples[idx.0].mintuple.tuple.t_infomask2 |= HEAP_TUPLE_HAS_MATCH;
        }
        Ok(())
    }

    pub fn exec_hash_accum_instrumentation<'mcx>(
        node: &mut HashJoinState<'mcx>,
    ) -> PgResult<()> {
        // ExecHashAccumInstrumentation(hashNode->hinstrument, hashtable),
        // allocating hinstrument first if needed (the ExecReScanHashJoin path).
        let table = node
            .hj_HashTable
            .take()
            .expect("exec_hash_accum_instrumentation: hj_HashTable is NULL");
        {
            let state = inner_hash_state(node);
            if state.hinstrument.is_none() {
                state.hinstrument =
                    Some(types_nodes::nodehash::HashInstrumentSlot::Local(mcx::alloc_in(
                        table.spillCxt,
                        types_nodes::nodehash::HashInstrumentation::default(),
                    )?));
            }
            let slot = state.hinstrument.as_mut().unwrap();
            instrument::with_hinstrument_mut(slot, |instr| {
                instrument::ExecHashAccumInstrumentation(instr, &table);
            });
        }
        node.hj_HashTable = Some(table);
        Ok(())
    }

    pub fn exec_build_hash32_expr<'mcx>(
        node: &mut HashJoinState<'mcx>,
        is_outer: bool,
        hashfuncids: &[types_core::primitive::Oid],
        collations: &[types_core::primitive::Oid],
        hash_exprs: &[types_nodes::primnodes::Expr],
        opstrict: &[bool],
        keep_nulls: bool,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
        // ExecBuildHash32Expr (execExpr.c:4302), called from nodeHashjoin.c's
        // ExecInitHashJoin to compile a per-side hash-value ExprState:
        //   hjstate->hj_OuterHash =
        //       ExecBuildHash32Expr(hjstate->js.ps.ps_ResultTupleDesc,
        //                           hjstate->js.ps.resultops, outer_hashfuncid,
        //                           node->hashcollations, node->hashkeys,
        //                           hash_strict, &hjstate->js.ps, 0,
        //                           HJ_FILL_OUTER(hjstate));
        //   hashstate->hash_expr =
        //       ExecBuildHash32Expr(hashstate->ps.ps_ResultTupleDesc,
        //                           hashstate->ps.resultops, inner_hashfuncid,
        //                           node->hashcollations, hash->hashkeys,
        //                           hash_strict, &hashstate->ps, 0,
        //                           HJ_FILL_INNER(hjstate));
        // The compiler lives in execExpr (real, ported builder). nodeHash owns
        // this seam only because the inner program is stored on the inner
        // HashState's `hash_expr`; we read each side's result desc/ops off its
        // PlanState and delegate the actual compilation to execExpr.
        let mcx = estate.es_query_cxt;

        // Per the C: `init_value` is 0 here; `keep_nulls` is the HJ_FILL_* flag.
        let init_value: u32 = 0;

        // Read the relevant side's result descriptor (`ps_ResultTupleDesc`) and
        // slot ops (`resultops`) off its PlanState — `js.ps` for the outer side,
        // the inner HashState's `ps` for the inner side — then delegate the
        // actual compilation to execExpr's `ExecBuildHash32Expr`.
        let ps = if is_outer {
            &node.js.ps
        } else {
            &inner_hash_state(node).ps
        };
        let desc = ps
            .ps_ResultTupleDesc
            .as_deref()
            .expect("exec_build_hash32_expr: ps_ResultTupleDesc is NULL");
        let ops = ps
            .resultops
            .expect("exec_build_hash32_expr: resultops is unset");
        // C threads `&hjstate->js.ps` as `parent`, reaching the EState for
        // SubPlan attribution; pass the non-owning EState back-link so a
        // correlated SubPlan in a hash key can find `es_subplanstates`.
        let es_link = types_nodes::execnodes::EStateLink::from_ref(estate);
        execExpr::exec_build_hash32_expr::call(
            mcx, es_link, desc, ops, hashfuncids, collations, hash_exprs, opstrict, init_value,
            keep_nulls,
        )
    }

    pub fn setup_skew_hashfunction<'mcx>(
        node: &mut HashJoinState<'mcx>,
        skew_hashfuncid: types_core::primitive::Oid,
        skew_collation: types_core::primitive::Oid,
        skew_table_valid: bool,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()> {
        // if (OidIsValid(hash->skewTable)) { skew_hashfunction = palloc0(FmgrInfo);
        //   skew_collation = ...; fmgr_info(outer_hashfuncid[0], skew_hashfunction); }
        if !skew_table_valid {
            return Ok(());
        }
        let mcx = estate.es_query_cxt;
        let mut fi = mcx::alloc_in(mcx, types_core::FmgrInfo::default())?;
        // fmgr_info(skew_hashfuncid, skew_hashfunction)
        fi.fn_oid = skew_hashfuncid;
        let state = inner_hash_state(node);
        state.skew_hashfunction = Some(fi);
        state.skew_collation = skew_collation;
        Ok(())
    }

    // ---- parallel-aware --------------------------------------------------

    pub fn exec_parallel_scan_hash_bucket<'mcx>(
        node: &mut HashJoinState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<bool> {
        parallel::ExecParallelScanHashBucket(node, estate)
    }

    pub fn exec_parallel_scan_hash_table_for_unmatched<'mcx>(
        node: &mut HashJoinState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<bool> {
        parallel::ExecParallelScanHashTableForUnmatched(node, estate)
    }

    pub fn exec_parallel_prep_hash_table_for_unmatched<'mcx>(
        node: &mut HashJoinState<'mcx>,
    ) -> PgResult<bool> {
        parallel::ExecParallelPrepHashTableForUnmatched(node)
    }

    pub fn exec_parallel_hash_table_alloc<'mcx>(
        node: &mut HashJoinState<'mcx>,
        batchno: i32,
    ) -> PgResult<()> {
        let mcx = ht(node).spillCxt;
        parallel::ExecParallelHashTableAlloc(mcx, ht(node), batchno)
    }

    pub fn exec_parallel_hash_table_set_current_batch<'mcx>(
        node: &mut HashJoinState<'mcx>,
        batchno: i32,
    ) -> PgResult<()> {
        parallel::ExecParallelHashTableSetCurrentBatch(ht(node), batchno);
        Ok(())
    }

    pub fn exec_parallel_hash_table_insert_current_batch<'mcx>(
        node: &mut HashJoinState<'mcx>,
        slot: SlotId,
        hashvalue: u32,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()> {
        let mcx = estate.es_query_cxt;
        let mut table = node
            .hj_HashTable
            .take()
            .expect("exec_parallel_hash_table_insert_current_batch: hj_HashTable is NULL");
        let res = parallel::ExecParallelHashTableInsertCurrentBatch(
            mcx, &mut table, estate, slot, hashvalue,
        );
        node.hj_HashTable = Some(table);
        res
    }

    pub fn exec_hash_table_detach<'mcx>(node: &mut HashJoinState<'mcx>) -> PgResult<()> {
        parallel::ExecHashTableDetach(ht(node))
    }

    pub fn exec_hash_table_detach_batch<'mcx>(node: &mut HashJoinState<'mcx>) -> PgResult<()> {
        parallel::ExecHashTableDetachBatch(ht(node))
    }

    pub fn force_store_minimal_into_slot<'mcx>(
        slot: SlotId,
        tuple: types_tuple::backend_access_common_heaptuple::FormedMinimalTuple<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()> {
        // ExecForceStoreMinimalTuple(tuple, slot, false) — owned by execTuples.
        backend_executor_execTuples_seams::exec_force_store_minimal_tuple::call(
            slot, tuple, false, estate,
        )
    }

    // ---- barrier / distributor / batch one-liners (parallel.rs helpers) --

    pub fn build_barrier_phase<'mcx>(node: &HashJoinState<'mcx>) -> i32 {
        parallel::build_barrier_phase(node)
    }
    pub fn build_barrier_arrive_and_wait<'mcx>(
        node: &mut HashJoinState<'mcx>,
        wait_event: u32,
    ) -> PgResult<bool> {
        parallel::build_barrier_arrive_and_wait(node, wait_event)
    }
    pub fn parallel_distributor_next_start<'mcx>(node: &HashJoinState<'mcx>) -> u32 {
        parallel::parallel_distributor_next_start(node)
    }
    pub fn parallel_batch_not_done<'mcx>(node: &HashJoinState<'mcx>, batchno: i32) -> bool {
        parallel::parallel_batch_not_done(node, batchno)
    }
    pub fn parallel_batch_attach<'mcx>(node: &mut HashJoinState<'mcx>, batchno: i32) -> i32 {
        parallel::parallel_batch_attach(node, batchno)
    }
    pub fn parallel_batch_arrive_and_wait<'mcx>(
        node: &mut HashJoinState<'mcx>,
        batchno: i32,
        wait_event: u32,
    ) -> PgResult<bool> {
        parallel::parallel_batch_arrive_and_wait(node, batchno, wait_event)
    }
    pub fn parallel_batch_detach<'mcx>(
        node: &mut HashJoinState<'mcx>,
        batchno: i32,
    ) -> PgResult<()> {
        parallel::parallel_batch_detach(node, batchno)
    }
    pub fn parallel_batch_phase<'mcx>(node: &HashJoinState<'mcx>, batchno: i32) -> i32 {
        parallel::parallel_batch_phase(node, batchno)
    }
    pub fn parallel_batch_set_done<'mcx>(
        node: &mut HashJoinState<'mcx>,
        batchno: i32,
    ) -> PgResult<()> {
        parallel::parallel_batch_set_done(node, batchno)
    }
    pub fn parallel_set_curbatch_invalid<'mcx>(node: &mut HashJoinState<'mcx>) -> PgResult<()> {
        parallel::parallel_set_curbatch_invalid(node)
    }
    pub fn parallel_has_curbatch<'mcx>(node: &HashJoinState<'mcx>) -> bool {
        parallel::parallel_has_curbatch(node)
    }

    /// `get_hash_memory_limit(void)` (nodeHash.c:3622) — reads the `work_mem` /
    /// `hash_mem_multiplier` backend GUCs and returns the per-hash memory budget
    /// in bytes. Consumed by Memoize and hash-agg spill via the seam.
    pub fn get_hash_memory_limit() -> PgResult<u64> {
        let (work_mem, hash_mem_multiplier) = parallel::hash_mem_gucs();
        Ok(hash_table::get_hash_memory_limit(work_mem, hash_mem_multiplier) as u64)
    }

    // Silence unused-import lint for the bucket-chain link helper imported for
    // documentation parity.
    #[allow(dead_code)]
    fn _uses(_l: HashJoinTupleLink) {}
    #[allow(dead_code)]
    fn _err() -> PgError {
        PgError::error("unused")
    }
}

// ===========================================================================
//                              Seam installation
// ===========================================================================

/// Install this unit's own outward-facing seams: the four parallel-context
/// node hooks (`backend-executor-nodeHash-pq-seams`) and all 34
/// `backend-executor-nodeHash-seams` the hash join drives.
pub fn init_seams() {
    use backend_executor_nodeHash_seams as s;

    // The four parallel-context node hooks. `execParallel` holds an opaque
    // `PlanStateHandle`, so these are installed as `PlanStateHandle`-typed shims
    // that recover the OWNED `&mut HashState` (via the unported PlanState
    // pointer registry) and call the real owned-typed entry points. The
    // `parallel_sup::hash_*` handle support-seams are RETIRED.
    instrument::init_pq_seams();

    s::exec_hash_table_create::set(adapters::exec_hash_table_create);
    s::multi_exec_hash::set(adapters::multi_exec_hash);
    s::exec_hash_table_destroy::set(adapters::exec_hash_table_destroy);
    s::exec_hash_table_reset::set(adapters::exec_hash_table_reset);
    s::exec_hash_table_reset_match_flags::set(adapters::exec_hash_table_reset_match_flags);
    s::exec_hash_table_insert::set(adapters::exec_hash_table_insert);
    s::exec_hash_get_bucket_and_batch::set(adapters::exec_hash_get_bucket_and_batch);
    s::exec_hash_get_skew_bucket::set(adapters::exec_hash_get_skew_bucket);
    s::exec_scan_hash_bucket::set(adapters::exec_scan_hash_bucket);
    s::exec_prep_hash_table_for_unmatched::set(adapters::exec_prep_hash_table_for_unmatched);
    s::exec_scan_hash_table_for_unmatched::set(adapters::exec_scan_hash_table_for_unmatched);
    s::cur_tuple_has_match::set(adapters::cur_tuple_has_match);
    s::cur_tuple_set_match::set(adapters::cur_tuple_set_match);
    s::exec_hash_accum_instrumentation::set(adapters::exec_hash_accum_instrumentation);
    s::exec_build_hash32_expr::set(adapters::exec_build_hash32_expr);
    s::setup_skew_hashfunction::set(adapters::setup_skew_hashfunction);
    s::exec_parallel_scan_hash_bucket::set(adapters::exec_parallel_scan_hash_bucket);
    s::exec_parallel_scan_hash_table_for_unmatched::set(
        adapters::exec_parallel_scan_hash_table_for_unmatched,
    );
    s::exec_parallel_prep_hash_table_for_unmatched::set(
        adapters::exec_parallel_prep_hash_table_for_unmatched,
    );
    s::exec_parallel_hash_table_alloc::set(adapters::exec_parallel_hash_table_alloc);
    s::exec_parallel_hash_table_set_current_batch::set(
        adapters::exec_parallel_hash_table_set_current_batch,
    );
    s::exec_parallel_hash_table_insert_current_batch::set(
        adapters::exec_parallel_hash_table_insert_current_batch,
    );
    s::exec_hash_table_detach::set(adapters::exec_hash_table_detach);
    s::exec_hash_table_detach_batch::set(adapters::exec_hash_table_detach_batch);
    s::force_store_minimal_into_slot::set(adapters::force_store_minimal_into_slot);
    s::build_barrier_phase::set(adapters::build_barrier_phase);
    s::build_barrier_arrive_and_wait::set(adapters::build_barrier_arrive_and_wait);
    s::parallel_distributor_next_start::set(adapters::parallel_distributor_next_start);
    s::parallel_batch_not_done::set(adapters::parallel_batch_not_done);
    s::parallel_batch_attach::set(adapters::parallel_batch_attach);
    s::parallel_batch_arrive_and_wait::set(adapters::parallel_batch_arrive_and_wait);
    s::parallel_batch_detach::set(adapters::parallel_batch_detach);
    s::parallel_batch_phase::set(adapters::parallel_batch_phase);
    s::parallel_batch_set_done::set(adapters::parallel_batch_set_done);
    s::parallel_set_curbatch_invalid::set(adapters::parallel_set_curbatch_invalid);
    s::parallel_has_curbatch::set(adapters::parallel_has_curbatch);
    s::get_hash_memory_limit::set(adapters::get_hash_memory_limit);

    // `ExecChooseHashTableSize` (nodeHash.c, owned here) is also read by
    // costsize.c's hashjoin sizing (`final_cost_hashjoin`) through the costsize
    // self-seam crate. The seam's `HashTableSize` carries only the
    // numbuckets/numbatches the cost model needs; map from the full owned result.
    backend_optimizer_path_costsize_seams::exec_choose_hash_table_size::set(
        |ntuples, tupwidth, useskew, try_combined_hash_mem, parallel_workers| {
            let r = hash_table::ExecChooseHashTableSize(
                ntuples,
                tupwidth,
                useskew,
                try_combined_hash_mem,
                parallel_workers,
            );
            backend_optimizer_path_costsize_seams::HashTableSize {
                numbuckets: r.numbuckets,
                numbatches: r.numbatches,
            }
        },
    );

    // `get_hash_memory_limit()` (nodeHash.c, owned here) is also read by the
    // hashjoin/memoize cost model through pathnode-seams as a bare `f64`; install
    // the owner body (costsize.c explicitly defers this to nodeHash).
    backend_optimizer_util_pathnode_seams::get_hash_memory_limit::set(|| {
        adapters::get_hash_memory_limit().expect("get_hash_memory_limit") as f64
    });
}

/// Silence the unused-`Size` import warning in the scaffold.
#[allow(dead_code)]
const _: fn() -> Size = || 0;
