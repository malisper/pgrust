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
    /// attach the worker to the existing shared-stats DSM chunk, installing it as
    /// `node->shared_info` so `ExecEndMemoize`'s worker copyback writes its slot
    /// into the canonical shared store.
    pub fn memoize_attach_shared_info(
        node: PlanStateHandle,
        chunk: types_execparallel::SerializeCursor,
    ) -> PgResult<()>
);

// ---------------------------------------------------------------------------
// Shared-memory (DSM) canonical store for `SharedMemoizeInfo`. The C code
// `shm_toc_allocate`s the chunk, writes `num_workers` and zero-fills the
// `sinstrument` array, and points `node->shared_info` directly at it; workers
// `shm_toc_lookup` the same chunk and write their slot in-place, and the leader
// `memcpy`s the chunk into local memory. These leaves reproduce that aliasing:
// the chunk is the canonical store, owned by the DSM/execParallel substrate
// (cf. `store_instrumentation_header` / `instrumentation_from_chunk`).
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `MemSet(node->shared_info, 0, size); node->shared_info->num_workers =
    /// pcxt->nworkers` — write the `SharedMemoizeInfo` header (`num_workers`) and
    /// a zeroed `sinstrument[num_workers]` array into the freshly-allocated DSM
    /// `chunk`, returning a `SharedMemoizeInfo` aliasing that chunk for the node
    /// to install as `shared_info`.
    pub fn store_shared_memoize_info(
        chunk: types_execparallel::SerializeCursor,
        num_workers: i32,
    ) -> PgResult<types_nodes::nodememoize::SharedMemoizeInfo>
);

seam_core::seam!(
    /// Read the `SharedMemoizeInfo` (header + per-worker `sinstrument`) out of an
    /// existing DSM `chunk` (`memcpy(si, node->shared_info, size)`), for the
    /// leader's `ExecMemoizeRetrieveInstrumentation` copy-out and the worker's
    /// attach.
    pub fn shared_memoize_info_from_chunk(
        chunk: types_execparallel::SerializeCursor,
    ) -> PgResult<types_nodes::nodememoize::SharedMemoizeInfo>
);

seam_core::seam!(
    /// Copy the node's current `shared_info` into freshly-`palloc`'d local memory
    /// and repoint `node->shared_info` at the copy
    /// (`si = palloc(size); memcpy(si, node->shared_info, size); node->shared_info
    /// = si`). After this the node owns its stats independently of the DSM chunk,
    /// which is torn down with the parallel context.
    pub fn memoize_copy_shared_info_local(node: PlanStateHandle) -> PgResult<()>
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
    /// (`&TTSOpsMinimalTuple`) and `mstate->probeslot` (`&TTSOpsVirtual`). In the
    /// owned model this resolves the `hashkeydesc` into `mstate.key_attrs` (the
    /// per-key `attbyval`/`attlen`) and sizes the owned slot value/null vectors
    /// (`table_values`/`table_isnull`/`probe_values`/`probe_isnull`) and the
    /// `param_exprs` / `hashfunctions` arrays (all `nkeys` long). All
    /// executor-owned (`ExecTypeFromExprList` / `MakeSingleTupleTableSlot`).
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
    /// `pslot->tts_values[i] = ExecEvalExpr(mstate->param_exprs[i], econtext,
    /// &pslot->tts_isnull[i])` (execExprInterp.c) — evaluate the `key_index`-th
    /// compiled cache-key parameter expression against the current scan
    /// parameters, in the node's per-tuple expression context. The
    /// `prepare_probe_slot(mstate, NULL)` orchestration (the per-key loop, the
    /// context switch, the slot framing) is in-crate; this is the genuine leaf
    /// call into the expression engine.
    pub fn eval_param_expr<'mcx>(
        mstate: &mut MemoizeScanState<'mcx>,
        key_index: usize,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<(types_datum::Datum, bool)>
);

seam_core::seam!(
    /// `slot_getallattrs` on a minimal-tuple slot holding `params`
    /// (`ExecStoreMinimalTuple(params, tslot, false); slot_getallattrs(tslot)`):
    /// deform a cached entry's key `params` into `numkeys` values/nulls. This is
    /// the genuine tuple-deform leaf (execTuples / heaptuple); the
    /// `prepare_probe_slot(mstate, key)` / `MemoizeHash_equal` orchestration that
    /// consumes the result is in-crate.
    pub fn deform_key_params<'mcx>(
        params: &MinimalTupleData<'mcx>,
        numkeys: usize,
        mcx: mcx::Mcx<'mcx>,
    ) -> PgResult<(alloc::vec::Vec<types_datum::Datum>, alloc::vec::Vec<bool>)>
);

seam_core::seam!(
    /// `DatumGetUInt32(FunctionCall1Coll(&hashfunctions[i], collation, value))`
    /// (fmgr.c) — the non-binary-mode per-key hash leaf. The rotating-XOR
    /// accumulation and `murmurhash32` finalization (`MemoizeHash_hash`) are
    /// in-crate; this is the catalog hash function invocation.
    pub fn function_call1_coll_uint32<'mcx>(
        mstate: &MemoizeScanState<'mcx>,
        key_index: usize,
        collation: types_core::Oid,
        value: types_datum::Datum,
    ) -> PgResult<u32>
);

seam_core::seam!(
    /// `datum_image_hash(value, attbyval, attlen)` (datum.c) — the binary-mode
    /// per-key hash leaf.
    pub fn datum_image_hash(
        value: types_datum::Datum,
        attbyval: bool,
        attlen: i16,
    ) -> PgResult<u32>
);

seam_core::seam!(
    /// `datum_image_eq(v1, v2, attbyval, attlen)` (datum.c) — the binary-mode
    /// per-key equality leaf used by `MemoizeHash_equal`.
    pub fn datum_image_eq(
        v1: types_datum::Datum,
        v2: types_datum::Datum,
        attbyval: bool,
        attlen: i16,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `ExecQual(mstate->cache_eq_expr, econtext)` (execExprInterp.c) with
    /// `econtext->ecxt_innertuple` = the table slot (the cached entry's deformed
    /// `params`, `table_values`/`table_isnull`) and `econtext->ecxt_outertuple` =
    /// the probe slot (`probe_values`/`probe_isnull`). The non-binary-mode
    /// equality leaf; the `MemoizeHash_equal` branch selection and the table-slot
    /// deform are in-crate.
    pub fn exec_qual_cache_eq<'mcx>(
        mstate: &mut MemoizeScanState<'mcx>,
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
