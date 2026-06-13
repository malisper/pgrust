//! Seam declarations for `backend-executor-nodeMemoize` (`nodeMemoize.c`, the
//! `Memoize` caching scan node).
//!
//! Two families of seams live here:
//!
//! 1. The parallel-executor entry points (`exec_memoize_*`) — the inward seams
//!    `backend-executor-execParallel` calls generically over the live
//!    `PlanState` tree (`PlanStateHandle`). The node crate owns and installs
//!    these.
//! 2. The genuinely-external operations the node reaches *downward* during init
//!    and run: the executor-owned slot/expr/context substrate (execUtils /
//!    execTuples / execExpr), the `simplehash` hash/equality leaves
//!    (`datum_image_hash` / `FunctionCall1Coll` / `ExecQual`), the catalog
//!    hash-function lookups (lsyscache), `fmgr_info`, the outer-child
//!    `ExecProcNode` / `ExecInitNode` / `ExecEndNode` / `ExecReScan` dispatch,
//!    and the cache memory budget (`get_hash_memory_limit`). Each defaults to a
//!    loud panic until its owner lands.
//!
//! The `tableContext` reset/delete is a genuine no-op in the owned-cache model
//! (the cache lives in the owned `MemoizeScanState.hashtable`, dropped by the
//! in-crate code), so those operations are folded into the in-crate logic
//! rather than seamed.

#![allow(unused_doc_comments)]
#![allow(non_snake_case)]

extern crate alloc;

use types_execparallel::{ParallelContextHandle, ParallelWorkerContextHandle, PlanStateHandle};

use types_error::PgResult;
use types_nodes::execnodes::EStateData;
use types_nodes::nodememoize::{Memoize, MemoizeScanState};
use types_tuple::heaptuple::MinimalTupleData;

// ===========================================================================
// Parallel-executor entry points (execParallel.c dispatch over PlanState).
// ===========================================================================

seam_core::seam!(pub fn exec_memoize_estimate(node: PlanStateHandle, pcxt: ParallelContextHandle) -> PgResult<()>);
seam_core::seam!(pub fn exec_memoize_initialize_dsm(node: PlanStateHandle, pcxt: ParallelContextHandle) -> PgResult<()>);
seam_core::seam!(pub fn exec_memoize_initialize_worker(node: PlanStateHandle, pwcxt: ParallelWorkerContextHandle) -> PgResult<()>);
seam_core::seam!(pub fn exec_memoize_retrieve_instrumentation(node: PlanStateHandle) -> PgResult<()>);

// ---------------------------------------------------------------------------
// Live-node accessors for the parallel paths (the leader / worker holds the
// concrete `MemoizeState` only as a `PlanStateHandle`). These resolve the
// handle to the owned node so the entry points above can read the instrument
// flag and read/write the `shared_info` mirror. Owned by whoever manages the
// live PlanState tree under parallel execution; panic loudly until it lands.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `node->ss.ps.instrument != NULL` for the Memoize node behind `node`.
    pub fn memoize_instrument_present(node: PlanStateHandle) -> PgResult<bool>
);

seam_core::seam!(
    /// `node->shared_info != NULL` for the Memoize node behind `node`.
    pub fn memoize_shared_info_present(node: PlanStateHandle) -> PgResult<bool>
);

seam_core::seam!(
    /// Install `node->shared_info` (the per-worker stats mirror) on the Memoize
    /// node behind `node`, stamped with `num_workers`.
    pub fn set_memoize_shared_info(
        node: PlanStateHandle,
        info: types_nodes::nodememoize::SharedMemoizeInfo,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `node->shared_info = shm_toc_lookup(pwcxt->toc, plan_node_id, true)` —
    /// attach the worker to the existing shared-stats DSM chunk. Reads the
    /// chunk's `num_workers` header and installs the node's mirror over it.
    pub fn memoize_attach_shared_info(
        node: PlanStateHandle,
        chunk: types_execparallel::SerializeCursor,
    ) -> PgResult<()>
);

// ===========================================================================
// Node factory / ExecInitMemoize executor wiring (execProcnode.c / execUtils.c
// / execTuples.c). These set up the executor-owned slots, expression states and
// expression context for the node; the in-crate `ExecInitMemoize` keeps the C
// control flow and calls these for each genuinely-external step. The new node
// is allocated in the estate's per-query context (`makeNode` in C).
// ===========================================================================

seam_core::seam!(
    /// `makeNode(MemoizeState)` — allocate and zero the executor-state node in
    /// the estate's per-query context, with the embedded `ScanState`/`PlanState`
    /// heads default-initialized.
    pub fn make_memoize_state<'mcx>(
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<alloc::boxed::Box<MemoizeScanState<'mcx>>>
);

seam_core::seam!(
    /// Wire `mstate->ss.ps.plan = (Plan *) node`, `mstate->ss.ps.state =
    /// estate` and install `mstate->ss.ps.ExecProcNode = ExecMemoize`.
    pub fn init_plan_state_links<'mcx>(
        mstate: &mut MemoizeScanState<'mcx>,
        node: &Memoize<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecAssignExprContext(estate, &mstate->ss.ps)`.
    pub fn exec_assign_expr_context<'mcx>(
        mstate: &mut MemoizeScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `outerPlanState(mstate) = ExecInitNode(outerPlan(node), estate, eflags)` —
    /// initialize the single outer child plan.
    pub fn init_outer_plan<'mcx>(
        mstate: &mut MemoizeScanState<'mcx>,
        node: &Memoize<'mcx>,
        estate: &mut EStateData<'mcx>,
        eflags: i32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecInitResultTupleSlotTL(&mstate->ss.ps, &TTSOpsMinimalTuple)` and
    /// `mstate->ss.ps.ps_ProjInfo = NULL`. The result slot is created into the
    /// estate's `es_tupleTable` arena.
    pub fn init_result_tuple_slot_tl<'mcx>(
        mstate: &mut MemoizeScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecCreateScanSlotFromOuterPlan(estate, &mstate->ss, &TTSOpsMinimalTuple)`.
    /// The scan slot is created into the estate's `es_tupleTable` arena.
    pub fn create_scan_slot_from_outer_plan<'mcx>(
        mstate: &mut MemoizeScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `mstate->hashkeydesc = ExecTypeFromExprList(node->param_exprs)` and build
    /// the two single tuple-table slots `mstate->tableslot`
    /// (`&TTSOpsMinimalTuple`) and `mstate->probeslot` (`&TTSOpsVirtual`), plus
    /// allocate the `param_exprs` / `hashfunctions` arrays (`nkeys` long). All
    /// executor-owned.
    pub fn init_hashkeydesc_and_slots<'mcx>(
        mstate: &mut MemoizeScanState<'mcx>,
        node: &Memoize<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `get_op_hash_functions(hashop, &left_hashfn, &right_hashfn)`
    /// (lsyscache.c) — `Some((left, right))` if found, `None` otherwise.
    pub fn get_op_hash_functions(
        hashop: types_core::Oid,
    ) -> PgResult<Option<(types_core::Oid, types_core::Oid)>>
);

seam_core::seam!(
    /// `fmgr_info(left_hashfn, &mstate->hashfunctions[key_index])` (fmgr.c).
    pub fn fmgr_info_hashfn<'mcx>(
        mstate: &mut MemoizeScanState<'mcx>,
        key_index: usize,
        left_hashfn: types_core::Oid,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `mstate->param_exprs[key_index] = ExecInitExpr(list_nth(node->param_exprs,
    /// key_index), (PlanState *) mstate)` (execExpr.c).
    pub fn exec_init_param_expr<'mcx>(
        mstate: &mut MemoizeScanState<'mcx>,
        node: &Memoize<'mcx>,
        key_index: usize,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `get_opcode(hashop)` (lsyscache.c) — the equality operator's underlying
    /// function OID.
    pub fn get_opcode(hashop: types_core::Oid) -> PgResult<types_core::Oid>
);

seam_core::seam!(
    /// `mstate->cache_eq_expr = ExecBuildParamSetEqual(mstate->hashkeydesc,
    /// &TTSOpsMinimalTuple, &TTSOpsVirtual, eqfuncoids, node->collations,
    /// node->param_exprs, (PlanState *) mstate)` (execExpr.c).
    pub fn build_cache_eq_expr<'mcx>(
        mstate: &mut MemoizeScanState<'mcx>,
        node: &Memoize<'mcx>,
        eqfuncoids: &[types_core::Oid],
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `get_hash_memory_limit()` (nodeHash.c) — the cache memory limit in bytes.
    pub fn get_hash_memory_limit() -> PgResult<u64>
);

// ===========================================================================
// Per-tuple run-time leaf operations (executor.h / execExprInterp.c /
// execTuples.c / datum.c / fmgr.c). These read the executor-owned slots and
// compiled expressions resolved from the node.
// ===========================================================================

seam_core::seam!(
    /// `CHECK_FOR_INTERRUPTS()` (miscadmin.h).
    pub fn check_for_interrupts() -> PgResult<()>
);

seam_core::seam!(
    /// `ResetExprContext(node->ss.ps.ps_ExprContext)` (executor.h) — reset the
    /// per-tuple memory context.
    pub fn reset_expr_context<'mcx>(
        mstate: &mut MemoizeScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `prepare_probe_slot(mstate, NULL)` — clear the probe slot and populate it
    /// by evaluating `mstate->param_exprs` against the current scan parameters in
    /// the per-tuple context, then `ExecStoreVirtualTuple`. Executor-owned slots.
    pub fn prepare_probe_from_params<'mcx>(
        mstate: &mut MemoizeScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `prepare_probe_slot(mstate, key)` — clear the probe slot, store `params`
    /// into the table slot, `slot_getallattrs`, copy its values/nulls into the
    /// probe slot, then `ExecStoreVirtualTuple`.
    pub fn prepare_probe_from_key<'mcx>(
        mstate: &mut MemoizeScanState<'mcx>,
        params: &MinimalTupleData<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `MemoizeHash_hash(tb, NULL)` — hash the current probe slot. In binary
    /// mode this `datum_image_hash`es each non-null key (rotating + XOR);
    /// otherwise it `FunctionCall1Coll`s each key's hash function. Returns the
    /// final `murmurhash32(hashkey)` value.
    pub fn hash_probe_slot<'mcx>(
        mstate: &mut MemoizeScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<u32>
);

seam_core::seam!(
    /// `MemoizeHash_equal(tb, key1, NULL)` — compare the cached entry `params`
    /// against the current probe slot. In binary mode this is a per-attribute
    /// `datum_image_eq`; otherwise it is `ExecQual(mstate->cache_eq_expr, ...)`.
    pub fn probe_equals_params<'mcx>(
        mstate: &mut MemoizeScanState<'mcx>,
        params: &MinimalTupleData<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `ExecCopySlotMinimalTuple(mstate->probeslot)` — copy the prepared probe
    /// slot's parameter values into a fresh owned `MinimalTuple` allocated in
    /// `mcx` (the cache's `tableContext`), used as a cache entry's key.
    pub fn copy_probe_slot_minimal_tuple<'mcx>(
        mstate: &mut MemoizeScanState<'mcx>,
        mcx: mcx::Mcx<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<MinimalTupleData<'mcx>>
);

// ===========================================================================
// Outer-child dispatch (execProcnode.c).
// ===========================================================================

seam_core::seam!(
    /// `outerslot = ExecProcNode(outerPlanState(node))` followed by
    /// `ExecCopySlotMinimalTuple(outerslot)` into `mcx`. Returns `Some(mintuple)`
    /// when a tuple is produced, or `None` when `TupIsNull(outerslot)`.
    pub fn exec_proc_outer<'mcx>(
        mstate: &mut MemoizeScanState<'mcx>,
        mcx: mcx::Mcx<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<MinimalTupleData<'mcx>>>
);

seam_core::seam!(
    /// `ExecEndNode(outerPlanState(node))` — shut down the outer child.
    pub fn exec_end_outer<'mcx>(
        mstate: &mut MemoizeScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecReScan(outerPlan)` — rescan the outer child.
    pub fn exec_rescan_outer<'mcx>(
        mstate: &mut MemoizeScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

// ===========================================================================
// Result-slot emission (execTuples.c). `ExecMemoize` returns a slot; here the
// result is stored into `node->ss.ps.ps_ResultTupleSlot`.
// ===========================================================================

seam_core::seam!(
    /// `ExecStoreMinimalTuple(tuple, node->ss.ps.ps_ResultTupleSlot, false)` —
    /// place the given cached/outer minimal tuple into the result slot (the
    /// result slot uses `TTSOpsMinimalTuple`, so this is equivalent to the C
    /// `ExecCopySlot` from the minimal-tuple outer slot).
    pub fn store_result_minimal_tuple<'mcx>(
        mstate: &mut MemoizeScanState<'mcx>,
        tuple: &MinimalTupleData<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecClearTuple(node->ss.ps.ps_ResultTupleSlot)` — clear the result slot,
    /// mirroring the C return of `NULL` from `ExecMemoize`.
    pub fn clear_result_slot<'mcx>(
        mstate: &mut MemoizeScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);
