//! Seam declarations for the executor/nodes support surface the parallel
//! executor (`execParallel.c`) reaches into.
//!
//! After the #169 de-handle, the executor objects the parallel executor threads
//! — the `PlanState` tree, the `EState`, the `QueryDesc` — are **owned** values
//! (`&mut PlanStateNode<'mcx>` / `&mut EStateData<'mcx>` / `&QueryDesc`), driven
//! by an enum-match tree walk and direct field access; the executor driver
//! (`execMain`) and the per-node parallel methods are called directly. The seams
//! that remain are the genuinely-external surfaces whose owners are not yet
//! ported (or not reachable without a cycle):
//!
//!   * worker **plan-shipping** (copyfuncs/outfuncs/readfuncs:
//!     `nodeToString`/`stringToNode` of the dummy `PlannedStmt`),
//!   * parameter / datum (de)serialization (`params.c` / `datum.c`),
//!   * `EState` reads that need owner logic (`GetPerTupleExprContext`,
//!     the resolved `es_param_exec_vals` value, the `QueryDesc` lifecycle
//!     accessors),
//!   * init-plan forcing (`ExecSetParamPlanMulti`, owned by nodeSubplan),
//!   * bitmapset membership, instrumentation accumulation, snapshot management,
//!     and `pgstat`/`miscadmin` reporting,
//!   * the orthogonal `ParallelContext`/`shm_toc` estimator access used by the
//!     per-node `Exec*Estimate` hooks (shared with nodeSort/nodeHash).
//!
//! The owning subsystems install them from their `init_seams()` when they land;
//! until then each call panics loudly.

#![allow(unused_doc_comments)]
#![allow(non_snake_case)]

use types_error::PgResult;
use types_execparallel::{
    ExprContextHandle, InstrumentationHandle, ParallelContextHandle, ParamExecValue,
    RestoredParam, SerializeCursor, Size,
};
use types_nodes::bitmapset::Bitmapset;
use types_nodes::querydesc::QueryDesc;
use types_nodes::EStateData;

// ===========================================================================
// Worker plan-shipping (copyfuncs.c / outfuncs.c / readfuncs.c / makefuncs.c).
// ===========================================================================

/// `ExecSerializePlan(planstate->plan, estate)` — the whole worker
/// plan-shipping pipeline: `copyObject(plan)` → clear the top target list's
/// `resjunk` → build the dummy `PlannedStmt` (field-fill + parallel-safe-subplan
/// filtering) → `nodeToString(pstmt)`. Reads the owned plan + `EState`; the plan
/// itself is reached through `estate->es_plannedstmt`. Owned by
/// copyfuncs/outfuncs (not yet ported); panics until they land.
seam_core::seam!(pub fn serialize_plan_for_workers(estate: &mut EStateData<'_>) -> PgResult<String>);

/// `CreateQueryDesc(stringToNode(pstmtspace), queryString, GetActiveSnapshot(),
/// InvalidSnapshot, receiver, RestoreParamList(...), NULL, instrument_options)`
/// — reconstruct the worker's owned `QueryDesc` from the serialized
/// `PlannedStmt` text. The `stringToNode` reconstruction is the worker
/// plan-shipping path (readfuncs.c); owned by readfuncs / execdesc (not yet
/// ported); panics until they land.
seam_core::seam!(pub fn create_parallel_query_desc<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    pstmt_text: String,
    query_string: String,
    receiver: types_execparallel::DestReceiverHandle,
    params: types_nodes::parsestmt::ParamListInfoHandle,
    instrument_options: i32,
) -> PgResult<QueryDesc>);

// ===========================================================================
// QueryDesc lifecycle accessors (execdesc.c — execdesc.h).
// ===========================================================================

/// `queryDesc->sourceText`.
seam_core::seam!(pub fn query_desc_source_text_owned(query_desc: &QueryDesc) -> PgResult<String>);
/// `queryDesc->plannedstmt->jitFlags = jit_flags`.
seam_core::seam!(pub fn set_query_desc_jit_flags_owned(query_desc: &mut QueryDesc, jit_flags: i32));
/// `queryDesc->estate->es_jit != NULL`.
seam_core::seam!(pub fn query_desc_estate_has_jit_owned(query_desc: &QueryDesc) -> bool);

// ===========================================================================
// EState reads that need owner logic (execUtils.c — execnodes.h).
// ===========================================================================

/// `GetPerTupleExprContext(estate)` — fetch (creating on first use) the
/// per-output-tuple `ExprContext`. Owned by execUtils; panics until it lands.
seam_core::seam!(pub fn get_per_tuple_expr_context_owned(estate: &mut EStateData<'_>) -> PgResult<ExprContextHandle>);

// ===========================================================================
// Init-plan forcing (nodeSubplan.c).
// ===========================================================================

/// `ExecSetParamPlanMulti(params, econtext)`. Owned by nodeSubplan; reached as a
/// seam to avoid the execParallel→nodeSubplan cycle.
seam_core::seam!(pub fn exec_set_param_plan_multi(
    params: &Bitmapset,
    econtext: ExprContextHandle,
) -> PgResult<()>);

// ===========================================================================
// Parameter (de)serialization (params.c) + datum (datum.c).
// ===========================================================================

/// `EstimateParamListSpace(paramLI)`.
seam_core::seam!(pub fn estimate_param_list_space(param_li: types_nodes::parsestmt::ParamListInfoHandle) -> Size);
/// `SerializeParamList(paramLI, &start_address)` into the chunk. Its
/// `get_typlenbyval` path can `ereport(ERROR)`, so the seam is fallible
/// (returns the advanced cursor on success).
seam_core::seam!(pub fn serialize_param_list(param_li: types_nodes::parsestmt::ParamListInfoHandle, chunk: SerializeCursor) -> PgResult<SerializeCursor>);
/// `RestoreParamList(&start_address)`.
seam_core::seam!(pub fn restore_param_list(chunk: SerializeCursor) -> types_nodes::parsestmt::ParamListInfoHandle);
/// `estate->es_param_exec_vals[paramid]` value/isnull + resolved type metadata.
// The serialized param value is a machine-word `Datum` read out of
// `es_param_exec_vals` and copied through the DSM chunk (params.c/datum.c);
// the carried `Datum`'s lifetime is unconstrained at this seam boundary
// (`'static`), matching the bare-word `datum_serialize`/`datum_restore`
// contract.
seam_core::seam!(pub fn param_exec_value_owned(estate: &mut EStateData<'_>, paramid: i32) -> ParamExecValue<'static>);
/// Write `{value, isnull}` back into `es_param_exec_vals[paramid]`, clearing `execPlan`.
seam_core::seam!(pub fn set_param_exec_value_owned<'mcx>(estate: &mut EStateData<'_>, paramid: i32, restored: RestoredParam<'mcx>));
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

// ===========================================================================
// Instrumentation slot init (instrument.c — for the leader's DSM header).
// ===========================================================================

/// `InstrInit(&GetInstrumentationArray(sei)[i], instrument_options)`.
seam_core::seam!(pub fn instr_init_slot(sei: InstrumentationHandle, i: i32, instrument_options: i32));
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
// Bitmapset membership (bitmapset.c).
// ===========================================================================

/// `bms_next_member(a, prevbit)`.
seam_core::seam!(pub fn bms_next_member(a: &Bitmapset, prevbit: i32) -> i32);
/// `bms_num_members(a)`.
seam_core::seam!(pub fn bms_num_members(a: &Bitmapset) -> i32);
/// `bms_is_empty(a)`.
seam_core::seam!(pub fn bms_is_empty(a: &Bitmapset) -> bool);

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
// Shared with nodeSort / nodeHash / nodeMemoize; kept (these address the
// DSM-owned ParallelContext, not the retired executor handles).
// ===========================================================================

/// `pcxt->nworkers`.
seam_core::seam!(pub fn pcxt_nworkers(pcxt: ParallelContextHandle) -> i32);
/// `shm_toc_estimate_chunk(&pcxt->estimator, size)`.
seam_core::seam!(pub fn pcxt_estimate_chunk(pcxt: ParallelContextHandle, size: Size) -> PgResult<()>);
/// `shm_toc_estimate_keys(&pcxt->estimator, keys)`.
seam_core::seam!(pub fn pcxt_estimate_keys(pcxt: ParallelContextHandle, keys: Size) -> PgResult<()>);
