//! Seam declarations for the executor/nodes support surface the parallel
//! executor (`execParallel.c`) reaches into: `PlanState`/`EState` field reads,
//! the `planstate_tree_walker`, the executor driver (`execMain.c`), the
//! `QueryDesc` lifecycle (`execdesc.c`), node (de)serialization
//! (`copyfuncs.c`/`outfuncs.c`/`readfuncs.c`), parameter and datum
//! (de)serialization (`params.c`/`datum.c`), instrumentation accumulation on a
//! `PlanState` (`instrument.c`), bitmapset membership (`bitmapset.c`), snapshot
//! management (`snapmgr.c`), and `pgstat`/`miscadmin` reporting.
//!
//! These operate on the `Copy` handles that name objects owned by sibling
//! subsystems not yet ported; the owning subsystems install them from their
//! `init_seams()` when they land. Until then each call panics loudly.

#![allow(unused_doc_comments)]
#![allow(non_snake_case)]

use types_error::PgResult;
use types_execparallel::{
    DsaAreaHandle, EStateHandle, ExprContextHandle, InstrumentationHandle, JitInstrumentation,
    JitInstrumentationHandle, ParallelContextHandle, ParallelWorkerContextHandle, ParamExecValue,
    ParamListInfoHandle, PlanHandle, PlanStateHandle, PlannedStmtHandle, QueryDescHandle,
    RestoredParam, SerializeCursor, Size, SnapshotHandle,
};
use types_nodes::bitmapset::Bitmapset;
use types_nodes::instrument::Instrumentation;
use types_nodes::nodes::NodeTag;

// ===========================================================================
// PlanState tree walk + node field reads (nodeFuncs.c / execnodes.h).
// ===========================================================================

/// Children of `planstate` in `planstate_tree_walker` order.
seam_core::seam!(pub fn planstate_children(planstate: PlanStateHandle) -> PgResult<Vec<PlanStateHandle>>);
/// `nodeTag(planstate)`.
seam_core::seam!(pub fn node_tag(planstate: PlanStateHandle) -> NodeTag);
/// `planstate->plan->parallel_aware`.
seam_core::seam!(pub fn plan_parallel_aware(planstate: PlanStateHandle) -> bool);
/// `planstate->plan->plan_node_id`.
seam_core::seam!(pub fn plan_node_id(planstate: PlanStateHandle) -> i32);
/// `planstate->plan`.
seam_core::seam!(pub fn planstate_plan(planstate: PlanStateHandle) -> PlanHandle);
/// `planstate->state`.
seam_core::seam!(pub fn planstate_estate(planstate: PlanStateHandle) -> EStateHandle);

// ===========================================================================
// EState field reads (execMain.c / execUtils.c — execnodes.h).
// ===========================================================================

/// `estate->es_param_list_info`.
seam_core::seam!(pub fn es_param_list_info(estate: EStateHandle) -> ParamListInfoHandle);
/// `estate->es_sourceText` — owned copy of the query string.
seam_core::seam!(pub fn es_source_text(estate: EStateHandle) -> PgResult<String>);
/// `estate->es_instrument` (instrument options bitmask).
seam_core::seam!(pub fn es_instrument(estate: EStateHandle) -> i32);
/// `estate->es_jit_flags`.
seam_core::seam!(pub fn es_jit_flags(estate: EStateHandle) -> i32);
/// `estate->es_top_eflags`.
seam_core::seam!(pub fn es_top_eflags(estate: EStateHandle) -> i32);
/// `estate->es_snapshot` (`None` when NULL).
seam_core::seam!(pub fn es_snapshot(estate: EStateHandle) -> Option<SnapshotHandle>);
/// `estate->es_query_dsa = area` (or `None` to clear).
seam_core::seam!(pub fn set_es_query_dsa(estate: EStateHandle, area: Option<DsaAreaHandle>));
/// `estate->es_jit != NULL`.
seam_core::seam!(pub fn es_has_jit(estate: EStateHandle) -> bool);
/// `estate->es_jit->instr`.
seam_core::seam!(pub fn es_jit_instr(estate: EStateHandle) -> JitInstrumentation);
/// `GetPerTupleExprContext(estate)`.
seam_core::seam!(pub fn get_per_tuple_expr_context(estate: EStateHandle) -> PgResult<ExprContextHandle>);
/// Allocate (if needed) and accumulate `add` into `estate->es_jit_worker_instr`
/// (the `MemoryContextAllocZero` + `InstrJitAgg(combined, add)`).
seam_core::seam!(pub fn accum_es_jit_worker_instr(estate: EStateHandle, add: JitInstrumentation));

// ===========================================================================
// Plan serialization (copyfuncs.c / outfuncs.c / readfuncs.c / makefuncs.c).
// ===========================================================================

/// `copyObject(plan)`.
seam_core::seam!(pub fn copy_plan(plan: PlanHandle) -> PlanHandle);
/// Clear `resjunk` on every entry of `plan->targetlist`.
seam_core::seam!(pub fn clear_plan_targetlist_resjunk(plan: PlanHandle));
/// Build the dummy `PlannedStmt` (`ExecSerializePlan`'s field-fill, including
/// the parallel-safe-subplan filtering that leaves NULL holes).
seam_core::seam!(pub fn build_serializable_plannedstmt(
    plan: PlanHandle,
    estate: EStateHandle,
) -> PgResult<PlannedStmtHandle>);
/// `nodeToString(pstmt)`.
seam_core::seam!(pub fn node_to_string(pstmt: PlannedStmtHandle) -> PgResult<String>);
/// `stringToNode(pstmtspace)` reconstructing a `PlannedStmt`.
seam_core::seam!(pub fn string_to_plannedstmt(s: String) -> PgResult<PlannedStmtHandle>);

// ===========================================================================
// Executor driver (execMain.c) + QueryDesc lifecycle (execdesc.c).
// ===========================================================================

/// `ExecutorStart(queryDesc, eflags)`.
seam_core::seam!(pub fn executor_start(query_desc: QueryDescHandle, eflags: i32) -> PgResult<()>);
/// `ExecutorRun(queryDesc, direction, count)`.
seam_core::seam!(pub fn executor_run(query_desc: QueryDescHandle, direction: i32, count: i64) -> PgResult<()>);
/// `ExecutorFinish(queryDesc)`.
seam_core::seam!(pub fn executor_finish(query_desc: QueryDescHandle) -> PgResult<()>);
/// `ExecutorEnd(queryDesc)`.
seam_core::seam!(pub fn executor_end(query_desc: QueryDescHandle) -> PgResult<()>);
/// `ExecSetParamPlanMulti(params, econtext)`.
seam_core::seam!(pub fn exec_set_param_plan_multi(
    params: &Bitmapset,
    econtext: ExprContextHandle,
) -> PgResult<()>);
/// `ExecSetTupleBound(tuples_needed, planstate)`.
seam_core::seam!(pub fn exec_set_tuple_bound(tuples_needed: i64, planstate: PlanStateHandle) -> PgResult<()>);
/// `CreateQueryDesc(pstmt, queryString, snapshot, crosscheck, dest, params, NULL, instrument_options)`.
seam_core::seam!(pub fn create_query_desc(
    pstmt: PlannedStmtHandle,
    query_string: String,
    snapshot: Option<SnapshotHandle>,
    crosscheck_snapshot: Option<SnapshotHandle>,
    receiver: types_execparallel::DestReceiverHandle,
    params: ParamListInfoHandle,
    instrument_options: i32,
) -> PgResult<QueryDescHandle>);
/// `FreeQueryDesc(queryDesc)`.
seam_core::seam!(pub fn free_query_desc(query_desc: QueryDescHandle));
/// `queryDesc->sourceText`.
seam_core::seam!(pub fn query_desc_source_text(query_desc: QueryDescHandle) -> PgResult<String>);
/// `queryDesc->estate`.
seam_core::seam!(pub fn query_desc_estate(query_desc: QueryDescHandle) -> EStateHandle);
/// `queryDesc->planstate`.
seam_core::seam!(pub fn query_desc_planstate(query_desc: QueryDescHandle) -> PlanStateHandle);
/// `queryDesc->plannedstmt->jitFlags = jit_flags`.
seam_core::seam!(pub fn set_query_desc_jit_flags(query_desc: QueryDescHandle, jit_flags: i32));

// ===========================================================================
// PlanState instrumentation field reads/writes (instrument.c — execnodes.h).
// ===========================================================================

/// `InstrAggNode(planstate->instrument, add)`.
seam_core::seam!(pub fn instr_agg_into_node(planstate: PlanStateHandle, add: Instrumentation));
/// `InstrAggNode(dst, planstate->instrument)` returning updated `dst`.
seam_core::seam!(pub fn instr_agg_node_value(dst: Instrumentation, planstate: PlanStateHandle) -> Instrumentation);
/// `InstrEndLoop(planstate->instrument)`.
seam_core::seam!(pub fn instr_end_loop(planstate: PlanStateHandle));
/// `planstate->worker_instrument = {num_workers, instrument[..]}`. The owner
/// (execMain) copies the slice into the per-query context.
seam_core::seam!(pub fn set_worker_instrument(
    planstate: PlanStateHandle,
    num_workers: i32,
    instrument: &[Instrumentation],
));
/// `planstate->worker_jit_instrument = {num_workers, jit_instr[..]}`. The owner
/// copies the slice into the per-query context.
seam_core::seam!(pub fn set_worker_jit_instrument(
    planstate: PlanStateHandle,
    num_workers: i32,
    jit_instr: &[JitInstrumentation],
));
/// `InstrInit(&GetInstrumentationArray(sei)[i], instrument_options)`.
seam_core::seam!(pub fn instr_init_slot(sei: InstrumentationHandle, i: i32, instrument_options: i32));
/// Read the `count` `Instrumentation` slots starting at `GetInstrumentationArray(sei)[base]`.
seam_core::seam!(pub fn sei_instrument_slots(
    sei: InstrumentationHandle,
    base: i32,
    count: i32,
) -> Vec<Instrumentation>);
/// `GetInstrumentationArray(sei)[index] = value`.
seam_core::seam!(pub fn sei_agg_into_slot(sei: InstrumentationHandle, index: i32, value: Instrumentation));
/// `shared_jit->jit_instr[index]`.
seam_core::seam!(pub fn shared_jit_instr(shared_jit: JitInstrumentationHandle, index: i32) -> JitInstrumentation);
/// `shared_jit->jit_instr[index] = value`.
seam_core::seam!(pub fn set_shared_jit_instr(
    shared_jit: JitInstrumentationHandle,
    index: i32,
    value: JitInstrumentation,
));
/// `InstrStartParallelQuery()`.
seam_core::seam!(pub fn instr_start_parallel_query());
/// `InstrEndParallelQuery(&bufusage[worker], &walusage[worker])`.
seam_core::seam!(pub fn instr_end_parallel_query(
    buffer_usage: SerializeCursor,
    wal_usage: SerializeCursor,
    worker: i32,
));
/// `InstrAccumParallelQuery(&bufusage[worker], &walusage[worker])`.
seam_core::seam!(pub fn instr_accum_parallel_query(
    buffer_usage: SerializeCursor,
    wal_usage: SerializeCursor,
    worker: i32,
));

// ===========================================================================
// Parameter (de)serialization (params.c) + datum (datum.c).
// ===========================================================================

/// `EstimateParamListSpace(paramLI)`.
seam_core::seam!(pub fn estimate_param_list_space(param_li: ParamListInfoHandle) -> Size);
/// `SerializeParamList(paramLI, &start_address)` into the chunk.
seam_core::seam!(pub fn serialize_param_list(param_li: ParamListInfoHandle, chunk: SerializeCursor));
/// `RestoreParamList(&start_address)`.
seam_core::seam!(pub fn restore_param_list(chunk: SerializeCursor) -> ParamListInfoHandle);
/// `estate->es_param_exec_vals[paramid]` value/isnull + resolved type metadata.
// The serialized param value is a machine-word `Datum` read out of
// `es_param_exec_vals` and copied through the DSM chunk (params.c/datum.c),
// not a live by-reference borrow into a memory context — so the carried
// `Datum`'s lifetime is unconstrained at this seam boundary (`'static`),
// matching the bare-word `datum_serialize`/`datum_restore` contract in
// `backend-utils-adt-datum-seams`.
seam_core::seam!(pub fn param_exec_value(estate: EStateHandle, paramid: i32) -> ParamExecValue<'static>);
/// Write `{value, isnull}` back into `es_param_exec_vals[paramid]`, clearing `execPlan`.
seam_core::seam!(pub fn set_param_exec_value<'mcx>(estate: EStateHandle, paramid: i32, restored: RestoredParam<'mcx>));
/// `datumEstimateSpace(value, isnull, typByVal, typLen)`.
seam_core::seam!(pub fn datum_estimate_space<'mcx>(prm: ParamExecValue<'mcx>) -> Size);
/// `datumSerialize(value, isnull, typByVal, typLen, &start_address)`.
seam_core::seam!(pub fn datum_serialize<'mcx>(prm: ParamExecValue<'mcx>, cursor: SerializeCursor) -> SerializeCursor);
/// `memcpy(start_address, &v, sizeof(int)); start_address += sizeof(int)`.
seam_core::seam!(pub fn datum_serialize_i32(cursor: SerializeCursor, v: i32) -> SerializeCursor);
/// `memcpy(&v, start_address, sizeof(int)); start_address += sizeof(int)`.
seam_core::seam!(pub fn datum_restore_i32(cursor: SerializeCursor) -> (i32, SerializeCursor));
/// `datumRestore(&start_address, &isnull)`.
seam_core::seam!(pub fn datum_restore(cursor: SerializeCursor) -> (RestoredParam<'static>, SerializeCursor));
/// Read a NUL-terminated string from the cursor.
seam_core::seam!(pub fn cursor_cstring(chunk: SerializeCursor) -> PgResult<String>);

// ===========================================================================
// Bitmapset membership (bitmapset.c).
// ===========================================================================

/// `bms_next_member(a, prevbit)`.
seam_core::seam!(pub fn bms_next_member(a: &Bitmapset, prevbit: i32) -> i32);
/// `bms_num_members(a)`.
seam_core::seam!(pub fn bms_num_members(a: &Bitmapset) -> i32);
/// `bms_is_empty(a)`.
seam_core::seam!(pub fn bms_is_empty(a: &Bitmapset) -> bool);

// ===========================================================================
// Snapshot management (snapmgr.c).
// ===========================================================================

/// `GetActiveSnapshot()`.
seam_core::seam!(pub fn get_active_snapshot() -> SnapshotHandle);

// ===========================================================================
// pgstat / misc reporting (utils/activity/*, tcop/postgres.c, miscadmin.h).
// ===========================================================================

/// `pgstat_report_activity(STATE_RUNNING, query)`.
seam_core::seam!(pub fn pgstat_report_activity_running(query: String));
/// `debug_query_string = str`.
seam_core::seam!(pub fn set_debug_query_string(s: String));

// ===========================================================================
// Orthogonal ParallelContext / shm_toc estimator access (access/parallel.h,
// storage/shm_toc.h) reached by the parallel-aware nodes' Exec*Estimate hooks.
// The per-node SortState field/shm seams that used to live here were RETIRED when
// nodeSort was re-keyed to take the owned `&mut SortStateData` (the node reads its
// own `ss.ps.instrument` / `shared_info` / `plan->plan_node_id` / `am_worker`
// directly); only the genuinely-orthogonal ParallelContext estimator handle
// remains, shared with nodeHash / nodeMemoize.
// ===========================================================================

/// `pcxt->nworkers`.
seam_core::seam!(pub fn pcxt_nworkers(pcxt: ParallelContextHandle) -> i32);
/// `shm_toc_estimate_chunk(&pcxt->estimator, size)`.
seam_core::seam!(pub fn pcxt_estimate_chunk(pcxt: ParallelContextHandle, size: Size) -> PgResult<()>);
/// `shm_toc_estimate_keys(&pcxt->estimator, keys)`.
seam_core::seam!(pub fn pcxt_estimate_keys(pcxt: ParallelContextHandle, keys: Size) -> PgResult<()>);

// ===========================================================================
// nodeMemoize's parallel-instrumentation hooks (ExecMemoize{Estimate,
// InitializeDSM,InitializeWorker,RetrieveInstrumentation}) were re-keyed off
// these handle-addressed MemoizeState/SharedMemoizeInfo accessors onto the owned
// `&mut MemoizeScanState<'mcx>` surface (rekey/nodeMemoize, mirroring the
// already-owned nodeAgg/nodeHashjoin/nodeBitmapHeapscan nodes). The DSM
// estimate/serialize now goes through the orthogonal owned shm_toc support seams
// in `backend-access-transam-parallel-seams` (which keep the DSM layout behind
// them), so the per-node `memoize_*` handle seams that used to live here are
// retired.
// ===========================================================================

// ===========================================================================
// Hash parallel-instrumentation support: RETIRED.
//
// The `hash_*` PlanStateHandle support-seams (hash_instrument_present /
// hash_plan_node_id / hash_shared_info_present / hash_shared_info_num_workers /
// hash_initialize_dsm_shared_info / hash_initialize_worker_shared_info /
// hash_retrieve_shared_info) were dropped by the nodeHash F0a re-key: the four
// parallel hooks (`ExecHashEstimate`/`ExecHashInitializeDSM`/
// `ExecHashInitializeWorker`/`ExecHashRetrieveInstrumentation`) now take the
// OWNED `&mut HashState` and read `ps.instrument`/`ps.plan->plan_node_id`
// directly, reaching the DSM-resident `SharedHashInfo` through the orthogonal
// owned `shm_toc_*` support-seams (in backend-access-transam-parallel-seams) +
// the typed shared-DSM-object flex primitive. No node/owner installed or
// consumed these handle seams.
// ===========================================================================

// The Append parallel support (`append_*`) seams have been RETIRED: nodeAppend's
// parallel surface (ExecAppendEstimate/InitializeDSM/ReInitializeDSM/Initialize-
// Worker) now takes the OWNED `&mut AppendStateData` and reads the node's fields
// directly, reaching the orthogonal DSM shm_toc via the
// `backend-access-transam-parallel` seams (the DSM owner), mirroring the
// already-owned nodeBitmapHeapscan/nodeHashjoin/nodeAgg parallel surfaces.
