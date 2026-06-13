//! Port of `execParallel.c` — the executor's parallel-query infrastructure
//! (PostgreSQL 18.3).
//!
//! Sets up, uses, and tears down a `ParallelContext` from within the executor:
//! serializes the plan + parameters into a DSM segment, walks the `PlanState`
//! tree to size and populate that segment, sets up the tuple queues the workers
//! write into, launches/reaps the workers, and aggregates their buffer/WAL
//! usage and instrumentation back into the leader.
//!
//! The orchestration (the `PlanState`-tree walks dispatching the per-node
//! parallel methods by node tag, the DSM-sizing / key-insertion sequence, the
//! per-worker instrumentation aggregation, the estimate/init bookkeeping) is
//! this crate's own logic. Everything below it — the DSM / `shm_toc` / `shm_mq`
//! primitives, the `ParallelContext` machinery, the tuple-queue reader/writer,
//! the DSA area, the per-node parallel methods, the executor driver,
//! `planstate_tree_walker`, parameter/datum (de)serialization, instrumentation,
//! snapshot management, and `pgstat` reporting — is owned by not-yet-ported
//! subsystems and reached through those owners' seam crates.
//!
//! The leader/worker-shared and externally-owned objects (the `PlanState`
//! tree, `EState`, `ParallelContext`, DSM segment, `shm_toc`, DSA area, tuple
//! queues, serialized `PlannedStmt`/`ParamListInfo`, `QueryDesc`,
//! `DestReceiver`) are threaded through the `Copy` handle newtypes in
//! [`types_execparallel`], exactly as the C threads a pointer.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use mcx::{Mcx, PgVec};
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR, ERRCODE_PROGRAM_LIMIT_EXCEEDED};
use types_execparallel::{
    dsa_pointer_is_valid, DsaAreaHandle, DsmSegmentHandle, EStateHandle,
    ExecParallelEstimateContext, ExecParallelInitializeDSMContext, FixedParallelExecutorState,
    InstrumentationHandle, JitInstrumentationHandle, ParallelContextHandle,
    ParallelExecutorInfo, ParallelWorkerContextHandle, PlanHandle, PlanStateHandle, QueryDescHandle,
    SerializeCursor, SharedExecutorInstrumentation, ShmTocHandle, Size, TuplesNeeded,
    DsaPointer, INVALID_DSA_POINTER, PGJIT_NONE,
};
use types_nodes::bitmapset::Bitmapset;
use types_nodes::execstate_tags::{
    T_AggState, T_AppendState, T_BitmapHeapScanState, T_BitmapIndexScanState, T_CustomScanState,
    T_ForeignScanState, T_HashJoinState, T_HashState, T_IncrementalSortState, T_IndexOnlyScanState,
    T_IndexScanState, T_MemoizeState, T_SeqScanState, T_SortState,
};
use types_core::instrument::{BufferUsage, WalUsage};
use types_nodes::instrument::Instrumentation;

use backend_access_transam_parallel_seams as parallel;
use backend_executor_execParallel_support_seams as sup;
use backend_executor_tqueue_seams as tqueue;
use backend_storage_ipc_shm_mq_seams as shmmq;
use backend_utils_mmgr_dsa_seams as dsa;

use backend_executor_nodeAgg_pq_seams as nodeAgg;
use backend_executor_nodeAppend_seams as nodeAppend;
use backend_executor_nodeBitmapHeapscan_seams as nodeBitmapHeap;
use backend_executor_nodeBitmapIndexscan_seams as nodeBitmapIndex;
use backend_executor_nodeCustom_seams as nodeCustom;
use backend_executor_nodeForeignscan_seams as nodeForeign;
use backend_executor_nodeHash_pq_seams as nodeHash;
use backend_executor_nodeHashjoin_pq_seams as nodeHashjoin;
use backend_executor_nodeIncrementalSort_seams as nodeIncrSort;
use backend_executor_nodeIndexonlyscan_seams as nodeIndexOnly;
use backend_executor_nodeIndexscan_pq_seams as nodeIndex;
use backend_executor_nodeMemoize_seams as nodeMemoize;
use backend_executor_nodeSeqscan_seams as nodeSeqscan;
use backend_executor_nodeSort_seams as nodeSort;

// ===========================================================================
// Magic numbers for parallel executor communication (execParallel.c:58-69).
// ===========================================================================

const PARALLEL_KEY_EXECUTOR_FIXED: u64 = 0xE000000000000001;
const PARALLEL_KEY_PLANNEDSTMT: u64 = 0xE000000000000002;
const PARALLEL_KEY_PARAMLISTINFO: u64 = 0xE000000000000003;
const PARALLEL_KEY_BUFFER_USAGE: u64 = 0xE000000000000004;
const PARALLEL_KEY_TUPLE_QUEUE: u64 = 0xE000000000000005;
const PARALLEL_KEY_INSTRUMENTATION: u64 = 0xE000000000000006;
const PARALLEL_KEY_DSA: u64 = 0xE000000000000007;
const PARALLEL_KEY_QUERY_TEXT: u64 = 0xE000000000000008;
const PARALLEL_KEY_JIT_INSTRUMENTATION: u64 = 0xE000000000000009;
const PARALLEL_KEY_WAL_USAGE: u64 = 0xE00000000000000A;

/// `PARALLEL_TUPLE_QUEUE_SIZE` (execParallel.c:69) — per-worker tuple queue size.
const PARALLEL_TUPLE_QUEUE_SIZE: Size = 65536;

/// `MAXIMUM_ALIGNOF` (pg_config.h) — 8 on the supported 64-bit platforms.
const MAXIMUM_ALIGNOF: usize = 8;

/// `MAXALIGN(LEN)` (c.h).
#[inline]
const fn maxalign(len: usize) -> usize {
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// `LWTRANCHE_PARALLEL_QUERY_DSA` (storage/lwlock.h) — the canonical
/// build-derived value (`NUM_INDIVIDUAL_LWLOCKS` + tranche offset = 71).
use types_storage::LWTRANCHE_PARALLEL_QUERY_DSA;

/// `ForwardScanDirection` (access/sdir.h) — value `1`.
const FORWARD_SCAN_DIRECTION: i32 = 1;

// offsetof(SharedExecutorInstrumentation, plan_node_id): four leading ints.
const OFFSET_OF_PLAN_NODE_ID: usize = 4 * core::mem::size_of::<i32>();
// offsetof(SharedJitInstrumentation, jit_instr): one leading int, MAXALIGNed.
const OFFSET_OF_JIT_INSTR: usize = maxalign(core::mem::size_of::<i32>());

const SIZEOF_INSTRUMENTATION: usize = core::mem::size_of::<Instrumentation>();
const SIZEOF_JIT_INSTRUMENTATION: usize =
    core::mem::size_of::<types_execparallel::JitInstrumentation>();
const SIZEOF_FIXED_STATE: usize = core::mem::size_of::<FixedParallelExecutorState>();
const SIZEOF_BUFFER_USAGE: usize = core::mem::size_of::<BufferUsage>();
const SIZEOF_WAL_USAGE: usize = core::mem::size_of::<WalUsage>();

/// `add_size(s1, s2)` (storage/shmem.c) — checked addition; raises the
/// "requested shared memory size overflows size_t" error on wraparound.
#[inline]
fn add_size(s1: Size, s2: Size) -> PgResult<Size> {
    s1.checked_add(s2).ok_or_else(size_overflow)
}

/// `mul_size(s1, s2)` (storage/shmem.c) — checked multiplication.
#[inline]
fn mul_size(s1: Size, s2: Size) -> PgResult<Size> {
    if s1 == 0 || s2 == 0 {
        return Ok(0);
    }
    s1.checked_mul(s2).ok_or_else(size_overflow)
}

fn size_overflow() -> PgError {
    PgError::error("requested shared memory size overflows size_t")
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
}

/// Install this crate's implementations into its seam slots.
pub fn init_seams() {
    backend_executor_execParallel_seams::ExecInitParallelPlan::set(ExecInitParallelPlan);
    backend_executor_execParallel_seams::ExecParallelCreateReaders::set(ExecParallelCreateReaders);
    backend_executor_execParallel_seams::ExecParallelReinitialize::set(ExecParallelReinitialize);
    backend_executor_execParallel_seams::ExecParallelFinish::set(ExecParallelFinish);
    backend_executor_execParallel_seams::ExecParallelCleanup::set(ExecParallelCleanup);
    backend_executor_execParallel_seams::ParallelQueryMain::set(ParallelQueryMain);
}

// ===========================================================================
// 1. ExecSerializePlan (execParallel.c:145-221)
// ===========================================================================

/// `ExecSerializePlan` — create a serialized representation of the plan to be
/// sent to each worker. Returns `nodeToString(pstmt)`.
fn ExecSerializePlan(plan: PlanHandle, estate: EStateHandle) -> PgResult<String> {
    // We can't scribble on the original plan, so make a copy.
    let plan = sup::copy_plan::call(plan);

    // The worker will start its own copy of the executor, and that copy will
    // insert a junk filter if the toplevel node has any resjunk entries. We
    // don't want that, because here the tuples are coming back to another
    // backend which may need them; clear resjunk on the target list.
    sup::clear_plan_targetlist_resjunk::call(plan);

    // Build the dummy PlannedStmt (field-fill + parallel-safe-subplan filtering
    // that leaves NULL holes), then serialize it.
    let pstmt = sup::build_serializable_plannedstmt::call(plan, estate)?;
    sup::node_to_string::call(pstmt)
}

// ===========================================================================
// 2. ExecParallelEstimate (execParallel.c:232-313)
// ===========================================================================

/// `ExecParallelEstimate` — per-node DSM-estimate tree walk. Counts the node,
/// dispatches the node's `ExecXxxEstimate` over its tag, then recurses.
fn ExecParallelEstimate(
    planstate: PlanStateHandle,
    e: &mut ExecParallelEstimateContext,
) -> PgResult<bool> {
    let pcxt = e.pcxt;

    // Count this node.
    e.nnodes += 1;

    let tag = sup::node_tag::call(planstate);
    if tag == T_SeqScanState {
        if sup::plan_parallel_aware::call(planstate) {
            nodeSeqscan::exec_seqscan_estimate::call(planstate, pcxt)?;
        }
    } else if tag == T_IndexScanState {
        nodeIndex::exec_indexscan_estimate::call(planstate, pcxt)?;
    } else if tag == T_IndexOnlyScanState {
        nodeIndexOnly::exec_indexonlyscan_estimate::call(planstate, pcxt)?;
    } else if tag == T_BitmapIndexScanState {
        nodeBitmapIndex::exec_bitmapindexscan_estimate::call(planstate, pcxt)?;
    } else if tag == T_ForeignScanState {
        if sup::plan_parallel_aware::call(planstate) {
            nodeForeign::exec_foreignscan_estimate::call(planstate, pcxt)?;
        }
    } else if tag == T_AppendState {
        if sup::plan_parallel_aware::call(planstate) {
            nodeAppend::exec_append_estimate::call(planstate, pcxt)?;
        }
    } else if tag == T_CustomScanState {
        if sup::plan_parallel_aware::call(planstate) {
            nodeCustom::exec_customscan_estimate::call(planstate, pcxt)?;
        }
    } else if tag == T_BitmapHeapScanState {
        if sup::plan_parallel_aware::call(planstate) {
            nodeBitmapHeap::exec_bitmapheap_estimate::call(planstate, pcxt)?;
        }
    } else if tag == T_HashJoinState {
        if sup::plan_parallel_aware::call(planstate) {
            nodeHashjoin::exec_hashjoin_estimate::call(planstate, pcxt)?;
        }
    } else if tag == T_HashState {
        nodeHash::exec_hash_estimate::call(planstate, pcxt)?;
    } else if tag == T_SortState {
        nodeSort::exec_sort_estimate::call(planstate, pcxt)?;
    } else if tag == T_IncrementalSortState {
        nodeIncrSort::exec_incrementalsort_estimate::call(planstate, pcxt)?;
    } else if tag == T_AggState {
        nodeAgg::exec_agg_estimate::call(planstate, pcxt)?;
    } else if tag == T_MemoizeState {
        nodeMemoize::exec_memoize_estimate::call(planstate, pcxt)?;
    }

    for child in sup::planstate_children::call(planstate)? {
        if ExecParallelEstimate(child, e)? {
            return Ok(true);
        }
    }
    Ok(false)
}

// ===========================================================================
// 3. EstimateParamExecSpace (execParallel.c:318-352)
// ===========================================================================

/// `EstimateParamExecSpace` — estimate the space required to serialize the
/// indicated PARAM_EXEC params.
fn EstimateParamExecSpace(estate: EStateHandle, params: &Bitmapset) -> PgResult<Size> {
    let mut sz: Size = core::mem::size_of::<i32>();

    let mut paramid: i32 = -1;
    loop {
        paramid = sup::bms_next_member::call(params, paramid);
        if paramid < 0 {
            break;
        }

        // value/isnull + resolved (typByVal, typLen): the C
        // `get_typlenbyval` / no-OID-by-value fallback folded into one read.
        let prm = sup::param_exec_value::call(estate, paramid);

        sz = add_size(sz, core::mem::size_of::<i32>())?; // space for paramid
        sz = add_size(sz, sup::datum_estimate_space::call(prm))?; // datum/isnull
    }
    Ok(sz)
}

// ===========================================================================
// 4. SerializeParamExecParams (execParallel.c:362-412)
// ===========================================================================

/// `SerializeParamExecParams` — serialize PARAM_EXEC parameters into DSA
/// storage; returns the DSA handle.
fn SerializeParamExecParams(
    estate: EStateHandle,
    params: &Bitmapset,
    area: DsaAreaHandle,
) -> PgResult<DsaPointer> {
    // Allocate enough space for the current parameter values.
    let size = EstimateParamExecSpace(estate, params)?;
    let handle = dsa::dsa_allocate::call(area, size);
    let mut cursor = dsa::dsa_get_address::call(area, handle);

    // First write the number of parameters as a 4-byte integer.
    let nparams: i32 = sup::bms_num_members::call(params);
    cursor = sup::datum_serialize_i32::call(cursor, nparams);

    let mut paramid: i32 = -1;
    loop {
        paramid = sup::bms_next_member::call(params, paramid);
        if paramid < 0 {
            break;
        }

        let prm = sup::param_exec_value::call(estate, paramid);

        // Write paramid, then datum/isnull.
        cursor = sup::datum_serialize_i32::call(cursor, paramid);
        cursor = sup::datum_serialize::call(prm, cursor);
    }

    Ok(handle)
}

// ===========================================================================
// 5. RestoreParamExecParams (execParallel.c:417-440)
// ===========================================================================

/// `RestoreParamExecParams` — restore PARAM_EXEC parameters from a serialized
/// buffer.
fn RestoreParamExecParams(cursor: SerializeCursor, estate: EStateHandle) {
    let (nparams, mut cursor) = sup::datum_restore_i32::call(cursor);

    for _ in 0..nparams {
        let (paramid, next) = sup::datum_restore_i32::call(cursor);
        cursor = next;

        let (restored, next) = sup::datum_restore::call(cursor);
        cursor = next;

        // prm->value/isnull = ...; prm->execPlan = NULL;
        sup::set_param_exec_value::call(estate, paramid, restored);
    }
}

// ===========================================================================
// 6. ExecParallelInitializeDSM (execParallel.c:446-540)
// ===========================================================================

/// `ExecParallelInitializeDSM` — per-node DSM-initialize tree walk.
fn ExecParallelInitializeDSM(
    planstate: PlanStateHandle,
    d: &mut ExecParallelInitializeDSMContext,
) -> PgResult<bool> {
    let pcxt = d.pcxt;

    // If instrumentation is enabled, initialize slot for this node.
    if let Some(sei) = d.instrumentation {
        parallel::set_sei_plan_node_id::call(sei, d.nnodes, sup::plan_node_id::call(planstate));
    }

    // Count this node.
    d.nnodes += 1;

    let tag = sup::node_tag::call(planstate);
    if tag == T_SeqScanState {
        if sup::plan_parallel_aware::call(planstate) {
            nodeSeqscan::exec_seqscan_initialize_dsm::call(planstate, pcxt)?;
        }
    } else if tag == T_IndexScanState {
        nodeIndex::exec_indexscan_initialize_dsm::call(planstate, pcxt)?;
    } else if tag == T_IndexOnlyScanState {
        nodeIndexOnly::exec_indexonlyscan_initialize_dsm::call(planstate, pcxt)?;
    } else if tag == T_BitmapIndexScanState {
        nodeBitmapIndex::exec_bitmapindexscan_initialize_dsm::call(planstate, pcxt)?;
    } else if tag == T_ForeignScanState {
        if sup::plan_parallel_aware::call(planstate) {
            nodeForeign::exec_foreignscan_initialize_dsm::call(planstate, pcxt)?;
        }
    } else if tag == T_AppendState {
        if sup::plan_parallel_aware::call(planstate) {
            nodeAppend::exec_append_initialize_dsm::call(planstate, pcxt)?;
        }
    } else if tag == T_CustomScanState {
        if sup::plan_parallel_aware::call(planstate) {
            nodeCustom::exec_customscan_initialize_dsm::call(planstate, pcxt)?;
        }
    } else if tag == T_BitmapHeapScanState {
        if sup::plan_parallel_aware::call(planstate) {
            nodeBitmapHeap::exec_bitmapheap_initialize_dsm::call(planstate, pcxt)?;
        }
    } else if tag == T_HashJoinState {
        if sup::plan_parallel_aware::call(planstate) {
            nodeHashjoin::exec_hashjoin_initialize_dsm::call(planstate, pcxt)?;
        }
    } else if tag == T_HashState {
        nodeHash::exec_hash_initialize_dsm::call(planstate, pcxt)?;
    } else if tag == T_SortState {
        nodeSort::exec_sort_initialize_dsm::call(planstate, pcxt)?;
    } else if tag == T_IncrementalSortState {
        nodeIncrSort::exec_incrementalsort_initialize_dsm::call(planstate, pcxt)?;
    } else if tag == T_AggState {
        nodeAgg::exec_agg_initialize_dsm::call(planstate, pcxt)?;
    } else if tag == T_MemoizeState {
        nodeMemoize::exec_memoize_initialize_dsm::call(planstate, pcxt)?;
    }

    for child in sup::planstate_children::call(planstate)? {
        if ExecParallelInitializeDSM(child, d)? {
            return Ok(true);
        }
    }
    Ok(false)
}

// ===========================================================================
// 7. ExecParallelSetupTupleQueues (execParallel.c:546-592)
// ===========================================================================

/// `ExecParallelSetupTupleQueues` — set up the response queues for workers to
/// return tuples to the leader and become the receiver for each. Returns the
/// `shm_mq_handle **` array (empty when there are no workers).
fn ExecParallelSetupTupleQueues<'mcx>(
    mcx: Mcx<'mcx>,
    pcxt: ParallelContextHandle,
    reinitialize: bool,
) -> PgResult<PgVec<'mcx, types_execparallel::ShmMqAttachHandle>> {
    let nworkers = parallel::pcxt_nworkers::call(pcxt);

    // Skip this if no workers.
    if nworkers == 0 {
        return Ok(PgVec::new_in(mcx));
    }

    let toc = parallel::pcxt_toc::call(pcxt);
    let seg = parallel::pcxt_seg::call(pcxt);

    // If not reinitializing, allocate space from the DSM for the queues;
    // otherwise, find the already allocated space.
    let tqueuespace: SerializeCursor = if !reinitialize {
        parallel::shm_toc_allocate::call(toc, mul_size(PARALLEL_TUPLE_QUEUE_SIZE, nworkers as usize)?)
    } else {
        parallel::shm_toc_lookup::call(toc, PARALLEL_KEY_TUPLE_QUEUE, false).ok_or_else(|| {
            PgError::error(
                "ExecParallelSetupTupleQueues: PARALLEL_KEY_TUPLE_QUEUE present (noError == false)",
            )
        })?
    };

    // Allocate the `nworkers`-sized handle array (C `palloc`). Create the
    // queues, and become the receiver for each.
    let mut responseq = mcx::vec_with_capacity_in(mcx, nworkers as usize)?;
    for i in 0..nworkers {
        let mq = shmmq::shm_mq_create_at::call(tqueuespace, i, PARALLEL_TUPLE_QUEUE_SIZE);
        shmmq::shm_mq_set_receiver_to_myproc::call(mq);
        responseq.push(shmmq::shm_mq_attach::call(mq, seg)?);
    }

    // Add array of queues to shm_toc, so others can find it.
    if !reinitialize {
        parallel::shm_toc_insert::call(toc, PARALLEL_KEY_TUPLE_QUEUE, tqueuespace);
    }

    Ok(responseq)
}

// ===========================================================================
// 8. ExecInitParallelPlan (execParallel.c:598-881)
// ===========================================================================

/// `ExecInitParallelPlan` — set up the infrastructure for workers to execute
/// and return results. Builds the [`ParallelExecutorInfo`], sizes + populates
/// the DSM, sets up tuple queues and (optionally) instrumentation, and creates
/// the DSA area.
pub fn ExecInitParallelPlan<'mcx>(
    mcx: Mcx<'mcx>,
    planstate: PlanStateHandle,
    estate: EStateHandle,
    send_params: &Bitmapset,
    nworkers: i32,
    tuples_needed: TuplesNeeded,
) -> PgResult<ParallelExecutorInfo<'mcx>> {
    let mut instrumentation_len: i32 = 0;
    let mut jit_instrumentation_len: i32 = 0;
    let mut instrument_offset: i32 = 0;
    let dsa_minsize: Size = dsa::dsa_minimum_size::call();

    // Force any initplan outputs to be evaluated, if they weren't already.
    let per_tuple_econtext = sup::get_per_tuple_expr_context::call(estate)?;
    sup::exec_set_param_plan_multi::call(send_params, per_tuple_econtext)?;

    // Allocate object for return value (palloc0 defaults).
    let mut pei = ParallelExecutorInfo {
        planstate,
        pcxt: None,
        buffer_usage: SerializeCursor(0),
        wal_usage: SerializeCursor(0),
        instrumentation: None,
        jit_instrumentation: None,
        area: None,
        param_exec: INVALID_DSA_POINTER,
        finished: false,
        tqueue: PgVec::new_in(mcx),
        reader: PgVec::new_in(mcx),
    };

    // Fix up and serialize plan to be sent to workers.
    let pstmt_data = ExecSerializePlan(sup::planstate_plan::call(planstate), estate)?;

    // Create a parallel context.
    let pcxt = parallel::create_parallel_context::call(
        mcx,
        String::from("postgres"),
        String::from("ParallelQueryMain"),
        nworkers,
    )?;
    pei.pcxt = Some(pcxt);

    let estimator = parallel::pcxt_estimator::call(pcxt);
    let pcxt_nworkers = parallel::pcxt_nworkers::call(pcxt);

    // Estimate space for fixed-size state.
    parallel::shm_toc_estimate_chunk::call(estimator, SIZEOF_FIXED_STATE);
    parallel::shm_toc_estimate_keys::call(estimator, 1);

    // Estimate space for query text.
    let query_text = sup::es_source_text::call(estate)?;
    let query_len: i32 = query_text.len() as i32;
    parallel::shm_toc_estimate_chunk::call(estimator, query_len as Size + 1);
    parallel::shm_toc_estimate_keys::call(estimator, 1);

    // Estimate space for serialized PlannedStmt.
    let pstmt_len: i32 = pstmt_data.len() as i32 + 1;
    parallel::shm_toc_estimate_chunk::call(estimator, pstmt_len as Size);
    parallel::shm_toc_estimate_keys::call(estimator, 1);

    // Estimate space for serialized ParamListInfo.
    let param_li = sup::es_param_list_info::call(estate);
    let paramlistinfo_len: i32 = sup::estimate_param_list_space::call(param_li) as i32;
    parallel::shm_toc_estimate_chunk::call(estimator, paramlistinfo_len as Size);
    parallel::shm_toc_estimate_keys::call(estimator, 1);

    // Estimate space for BufferUsage.
    parallel::shm_toc_estimate_chunk::call(
        estimator,
        mul_size(SIZEOF_BUFFER_USAGE, pcxt_nworkers as usize)?,
    );
    parallel::shm_toc_estimate_keys::call(estimator, 1);

    // Same for WalUsage.
    parallel::shm_toc_estimate_chunk::call(estimator, mul_size(SIZEOF_WAL_USAGE, pcxt_nworkers as usize)?);
    parallel::shm_toc_estimate_keys::call(estimator, 1);

    // Estimate space for tuple queues.
    parallel::shm_toc_estimate_chunk::call(
        estimator,
        mul_size(PARALLEL_TUPLE_QUEUE_SIZE, pcxt_nworkers as usize)?,
    );
    parallel::shm_toc_estimate_keys::call(estimator, 1);

    // Give parallel-aware nodes a chance to add to the estimates, and count
    // how many PlanState nodes there are.
    let mut e = ExecParallelEstimateContext { pcxt, nnodes: 0 };
    ExecParallelEstimate(planstate, &mut e)?;

    // Estimate space for instrumentation, if required.
    let es_instrument = sup::es_instrument::call(estate);
    let es_jit_flags = sup::es_jit_flags::call(estate);
    if es_instrument != 0 {
        instrumentation_len =
            OFFSET_OF_PLAN_NODE_ID as i32 + core::mem::size_of::<i32>() as i32 * e.nnodes;
        instrumentation_len = maxalign(instrumentation_len as usize) as i32;
        instrument_offset = instrumentation_len;
        instrumentation_len += mul_size(
            SIZEOF_INSTRUMENTATION,
            mul_size(e.nnodes as usize, nworkers as usize)?,
        )? as i32;
        parallel::shm_toc_estimate_chunk::call(estimator, instrumentation_len as Size);
        parallel::shm_toc_estimate_keys::call(estimator, 1);

        // Estimate space for JIT instrumentation, if required.
        if es_jit_flags != PGJIT_NONE {
            jit_instrumentation_len =
                OFFSET_OF_JIT_INSTR as i32 + SIZEOF_JIT_INSTRUMENTATION as i32 * nworkers;
            parallel::shm_toc_estimate_chunk::call(estimator, jit_instrumentation_len as Size);
            parallel::shm_toc_estimate_keys::call(estimator, 1);
        }
    }

    // Estimate space for DSA area.
    parallel::shm_toc_estimate_chunk::call(estimator, dsa_minsize);
    parallel::shm_toc_estimate_keys::call(estimator, 1);

    // InitializeParallelDSM passes the active snapshot to the worker, which
    // uses it to set es_snapshot. Make sure we don't set es_snapshot
    // differently in the child.
    debug_assert!(sup::es_snapshot::call(estate) == Some(sup::get_active_snapshot::call()));

    // Everyone's had a chance to ask for space, so now create the DSM.
    parallel::initialize_parallel_dsm::call(mcx, pcxt)?;

    let toc = parallel::pcxt_toc::call(pcxt);
    let seg = parallel::pcxt_seg::call(pcxt);

    // Store fixed-size state.
    let fpes_chunk = parallel::shm_toc_allocate::call(toc, SIZEOF_FIXED_STATE);
    let fpes = parallel::store_fixed_state::call(
        fpes_chunk,
        FixedParallelExecutorState {
            tuples_needed,
            param_exec: INVALID_DSA_POINTER,
            eflags: sup::es_top_eflags::call(estate),
            jit_flags: es_jit_flags,
        },
    );
    parallel::shm_toc_insert::call(toc, PARALLEL_KEY_EXECUTOR_FIXED, fpes_chunk);

    // Store query string.
    let query_chunk = parallel::shm_toc_allocate::call(toc, query_len as Size + 1);
    parallel::store_cstring::call(query_chunk, query_text);
    parallel::shm_toc_insert::call(toc, PARALLEL_KEY_QUERY_TEXT, query_chunk);

    // Store serialized PlannedStmt.
    let pstmt_chunk = parallel::shm_toc_allocate::call(toc, pstmt_len as Size);
    parallel::store_cstring::call(pstmt_chunk, pstmt_data);
    parallel::shm_toc_insert::call(toc, PARALLEL_KEY_PLANNEDSTMT, pstmt_chunk);

    // Store serialized ParamListInfo.
    let paramlistinfo_chunk = parallel::shm_toc_allocate::call(toc, paramlistinfo_len as Size);
    parallel::shm_toc_insert::call(toc, PARALLEL_KEY_PARAMLISTINFO, paramlistinfo_chunk);
    sup::serialize_param_list::call(param_li, paramlistinfo_chunk);

    // Allocate space for each worker's BufferUsage; no need to initialize.
    let bufusage_chunk =
        parallel::shm_toc_allocate::call(toc, mul_size(SIZEOF_BUFFER_USAGE, pcxt_nworkers as usize)?);
    parallel::shm_toc_insert::call(toc, PARALLEL_KEY_BUFFER_USAGE, bufusage_chunk);
    pei.buffer_usage = bufusage_chunk;

    // Same for WalUsage.
    let walusage_chunk =
        parallel::shm_toc_allocate::call(toc, mul_size(SIZEOF_WAL_USAGE, pcxt_nworkers as usize)?);
    parallel::shm_toc_insert::call(toc, PARALLEL_KEY_WAL_USAGE, walusage_chunk);
    pei.wal_usage = walusage_chunk;

    // Set up the tuple queues that the workers will write into.
    pei.tqueue = ExecParallelSetupTupleQueues(mcx, pcxt, false)?;

    // We don't need the TupleQueueReaders yet, though.
    pei.reader = PgVec::new_in(mcx);

    // If instrumentation options were supplied, allocate space for the data.
    if es_instrument != 0 {
        let instr_chunk = parallel::shm_toc_allocate::call(toc, instrumentation_len as Size);
        let instrumentation = parallel::store_instrumentation_header::call(
            instr_chunk,
            SharedExecutorInstrumentation {
                instrument_options: es_instrument,
                instrument_offset,
                num_workers: nworkers,
                num_plan_nodes: e.nnodes,
            },
        );
        for i in 0..(nworkers * e.nnodes) {
            sup::instr_init_slot::call(instrumentation, i, es_instrument);
        }
        parallel::shm_toc_insert::call(toc, PARALLEL_KEY_INSTRUMENTATION, instr_chunk);
        pei.instrumentation = Some(instrumentation);

        if es_jit_flags != PGJIT_NONE {
            let jit_chunk = parallel::shm_toc_allocate::call(toc, jit_instrumentation_len as Size);
            let jit_instrumentation =
                parallel::store_jit_instrumentation_header::call(jit_chunk, nworkers);
            parallel::shm_toc_insert::call(toc, PARALLEL_KEY_JIT_INSTRUMENTATION, jit_chunk);
            pei.jit_instrumentation = Some(jit_instrumentation);
        }
    }

    // Create a DSA area usable by the leader and all workers. (If we failed to
    // create a DSM and are using private memory instead, skip this.)
    if let Some(seg) = seg {
        let area_chunk = parallel::shm_toc_allocate::call(toc, dsa_minsize);
        parallel::shm_toc_insert::call(toc, PARALLEL_KEY_DSA, area_chunk);
        let area =
            dsa::dsa_create_in_place::call(area_chunk, dsa_minsize, LWTRANCHE_PARALLEL_QUERY_DSA, seg);
        pei.area = Some(area);

        // Serialize parameters, if any, using DSA storage.
        if !sup::bms_is_empty::call(send_params) {
            pei.param_exec = SerializeParamExecParams(estate, send_params, area)?;
            parallel::set_fixed_param_exec::call(fpes, pei.param_exec);
        }
    }

    // Give parallel-aware nodes a chance to initialize their shared data.
    let mut d = ExecParallelInitializeDSMContext {
        pcxt,
        instrumentation: pei.instrumentation,
        nnodes: 0,
    };

    // Install our DSA area while initializing the plan.
    sup::set_es_query_dsa::call(estate, pei.area);
    ExecParallelInitializeDSM(planstate, &mut d)?;
    sup::set_es_query_dsa::call(estate, None);

    // Make sure that the world hasn't shifted under our feet.
    if e.nnodes != d.nnodes {
        return Err(PgError::error("inconsistent count of PlanState nodes")
            .with_sqlstate(ERRCODE_INTERNAL_ERROR));
    }

    Ok(pei)
}

// ===========================================================================
// 9. ExecParallelCreateReaders (execParallel.c:889-909)
// ===========================================================================

/// `ExecParallelCreateReaders` — set up tuple queue readers to read the results
/// of a parallel subplan.
pub fn ExecParallelCreateReaders<'mcx>(
    mcx: Mcx<'mcx>,
    pei: &mut ParallelExecutorInfo<'mcx>,
) -> PgResult<()> {
    let pcxt = pei
        .pcxt
        .ok_or_else(|| PgError::error("ExecParallelCreateReaders: pei->pcxt is live"))?;
    let nworkers = parallel::pcxt_nworkers_launched::call(pcxt);

    debug_assert!(pei.reader.is_empty());

    if nworkers > 0 {
        let mut reader = mcx::vec_with_capacity_in(mcx, nworkers as usize)?;
        for i in 0..nworkers {
            let tqueue_i = pei.tqueue[i as usize];
            shmmq::shm_mq_set_handle::call(tqueue_i, parallel::pcxt_worker_bgwhandle::call(pcxt, i));
            reader.push(tqueue::create_tuple_queue_reader::call(tqueue_i));
        }
        pei.reader = reader;
    }
    Ok(())
}

// ===========================================================================
// 10. ExecParallelReinitialize (execParallel.c:915-959)
// ===========================================================================

/// `ExecParallelReinitialize` — re-initialize the parallel executor shared
/// memory state before launching a fresh batch of workers.
pub fn ExecParallelReinitialize<'mcx>(
    mcx: Mcx<'mcx>,
    planstate: PlanStateHandle,
    pei: &mut ParallelExecutorInfo<'mcx>,
    send_params: &Bitmapset,
) -> PgResult<()> {
    let estate = sup::planstate_estate::call(planstate);

    // Old workers must already be shut down.
    debug_assert!(pei.finished);

    // Force any initplan outputs to be evaluated, if they weren't already.
    let per_tuple_econtext = sup::get_per_tuple_expr_context::call(estate)?;
    sup::exec_set_param_plan_multi::call(send_params, per_tuple_econtext)?;

    let pcxt = pei
        .pcxt
        .ok_or_else(|| PgError::error("ExecParallelReinitialize: pei->pcxt is live"))?;
    parallel::reinitialize_parallel_dsm::call(pcxt)?;
    pei.tqueue = ExecParallelSetupTupleQueues(mcx, pcxt, true)?;
    pei.reader = PgVec::new_in(mcx);
    pei.finished = false;

    let toc = parallel::pcxt_toc::call(pcxt);
    let fpes_chunk = parallel::shm_toc_lookup::call(toc, PARALLEL_KEY_EXECUTOR_FIXED, false)
        .ok_or_else(|| PgError::error("ExecParallelReinitialize: PARALLEL_KEY_EXECUTOR_FIXED present"))?;
    let fpes = parallel::fixed_state_from_chunk::call(fpes_chunk);

    // Free any serialized parameters from the last round.
    if dsa_pointer_is_valid(parallel::fixed_param_exec::call(fpes)) {
        let area = pei
            .area
            .ok_or_else(|| PgError::error("ExecParallelReinitialize: pei->area is live during reinit"))?;
        dsa::dsa_free::call(area, parallel::fixed_param_exec::call(fpes));
        parallel::set_fixed_param_exec::call(fpes, INVALID_DSA_POINTER);
    }

    // Serialize current parameter values if required.
    if !sup::bms_is_empty::call(send_params) {
        let area = pei
            .area
            .ok_or_else(|| PgError::error("ExecParallelReinitialize: pei->area is live during reinit"))?;
        pei.param_exec = SerializeParamExecParams(estate, send_params, area)?;
        parallel::set_fixed_param_exec::call(fpes, pei.param_exec);
    }

    // Traverse plan tree and let each child node reset associated state.
    sup::set_es_query_dsa::call(estate, pei.area);
    ExecParallelReInitializeDSM(planstate, pcxt)?;
    sup::set_es_query_dsa::call(estate, None);
    Ok(())
}

// ===========================================================================
// 11. ExecParallelReInitializeDSM (execParallel.c:964-1028)
// ===========================================================================

/// `ExecParallelReInitializeDSM` — per-node DSM-reinitialize tree walk.
fn ExecParallelReInitializeDSM(
    planstate: PlanStateHandle,
    pcxt: ParallelContextHandle,
) -> PgResult<bool> {
    let tag = sup::node_tag::call(planstate);
    if tag == T_SeqScanState {
        if sup::plan_parallel_aware::call(planstate) {
            nodeSeqscan::exec_seqscan_reinitialize_dsm::call(planstate, pcxt)?;
        }
    } else if tag == T_IndexScanState {
        if sup::plan_parallel_aware::call(planstate) {
            nodeIndex::exec_indexscan_reinitialize_dsm::call(planstate, pcxt)?;
        }
    } else if tag == T_IndexOnlyScanState {
        if sup::plan_parallel_aware::call(planstate) {
            nodeIndexOnly::exec_indexonlyscan_reinitialize_dsm::call(planstate, pcxt)?;
        }
    } else if tag == T_ForeignScanState {
        if sup::plan_parallel_aware::call(planstate) {
            nodeForeign::exec_foreignscan_reinitialize_dsm::call(planstate, pcxt)?;
        }
    } else if tag == T_AppendState {
        if sup::plan_parallel_aware::call(planstate) {
            nodeAppend::exec_append_reinitialize_dsm::call(planstate, pcxt)?;
        }
    } else if tag == T_CustomScanState {
        if sup::plan_parallel_aware::call(planstate) {
            nodeCustom::exec_customscan_reinitialize_dsm::call(planstate, pcxt)?;
        }
    } else if tag == T_BitmapHeapScanState {
        if sup::plan_parallel_aware::call(planstate) {
            nodeBitmapHeap::exec_bitmapheap_reinitialize_dsm::call(planstate, pcxt)?;
        }
    } else if tag == T_HashJoinState {
        if sup::plan_parallel_aware::call(planstate) {
            nodeHashjoin::exec_hashjoin_reinitialize_dsm::call(planstate, pcxt)?;
        }
    } else if tag == T_BitmapIndexScanState
        || tag == T_HashState
        || tag == T_SortState
        || tag == T_IncrementalSortState
        || tag == T_MemoizeState
    {
        // these nodes have DSM state, but no reinitialization is required
    }

    for child in sup::planstate_children::call(planstate)? {
        if ExecParallelReInitializeDSM(child, pcxt)? {
            return Ok(true);
        }
    }
    Ok(false)
}

// ===========================================================================
// 12. ExecParallelRetrieveInstrumentation (execParallel.c:1034-1110)
// ===========================================================================

/// `ExecParallelRetrieveInstrumentation` — copy instrumentation about this node
/// and its descendants from DSM into the leader's `PlanState`.
fn ExecParallelRetrieveInstrumentation<'mcx>(
    mcx: Mcx<'mcx>,
    planstate: PlanStateHandle,
    instrumentation: InstrumentationHandle,
) -> PgResult<bool> {
    let plan_node_id = sup::plan_node_id::call(planstate);

    // Find the instrumentation for this node.
    let num_plan_nodes = parallel::sei_num_plan_nodes::call(instrumentation);
    let num_workers = parallel::sei_num_workers::call(instrumentation);
    let mut i: i32 = 0;
    while i < num_plan_nodes {
        if parallel::sei_plan_node_id::call(instrumentation, i) == plan_node_id {
            break;
        }
        i += 1;
    }
    if i >= num_plan_nodes {
        return Err(PgError::error(format!("plan node {plan_node_id} not found"))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR));
    }

    // Accumulate the statistics from all workers.
    let base = i * num_workers;
    let worker_slots = sup::sei_instrument_slots::call(instrumentation, base, num_workers);
    for slot in &worker_slots {
        sup::instr_agg_into_node::call(planstate, *slot);
    }

    // Also store the per-worker detail (allocated in per-query context).
    sup::set_worker_instrument::call(planstate, num_workers, &worker_slots);

    // Perform any node-type-specific work that needs to be done.
    let tag = sup::node_tag::call(planstate);
    if tag == T_IndexScanState {
        nodeIndex::exec_indexscan_retrieve_instrumentation::call(planstate)?;
    } else if tag == T_IndexOnlyScanState {
        nodeIndexOnly::exec_indexonlyscan_retrieve_instrumentation::call(planstate)?;
    } else if tag == T_BitmapIndexScanState {
        nodeBitmapIndex::exec_bitmapindexscan_retrieve_instrumentation::call(planstate)?;
    } else if tag == T_SortState {
        nodeSort::exec_sort_retrieve_instrumentation::call(planstate)?;
    } else if tag == T_IncrementalSortState {
        nodeIncrSort::exec_incrementalsort_retrieve_instrumentation::call(planstate)?;
    } else if tag == T_HashState {
        nodeHash::exec_hash_retrieve_instrumentation::call(planstate)?;
    } else if tag == T_AggState {
        nodeAgg::exec_agg_retrieve_instrumentation::call(planstate)?;
    } else if tag == T_MemoizeState {
        nodeMemoize::exec_memoize_retrieve_instrumentation::call(planstate)?;
    } else if tag == T_BitmapHeapScanState {
        nodeBitmapHeap::exec_bitmapheap_retrieve_instrumentation::call(planstate)?;
    }

    for child in sup::planstate_children::call(planstate)? {
        if ExecParallelRetrieveInstrumentation(mcx, child, instrumentation)? {
            return Ok(true);
        }
    }
    Ok(false)
}

// ===========================================================================
// 13. ExecParallelRetrieveJitInstrumentation (execParallel.c:1115-1149)
// ===========================================================================

/// `ExecParallelRetrieveJitInstrumentation` — add up the workers' JIT
/// instrumentation from DSM.
fn ExecParallelRetrieveJitInstrumentation<'mcx>(
    mcx: Mcx<'mcx>,
    planstate: PlanStateHandle,
    shared_jit: JitInstrumentationHandle,
) -> PgResult<()> {
    let estate = sup::planstate_estate::call(planstate);

    // Accumulate worker JIT instrumentation into the combined JIT
    // instrumentation, allocating it if required, folding each worker in; build
    // the per-worker detail array in per-query context.
    let shared_jit_num_workers = parallel::shared_jit_num_workers::call(shared_jit);
    let mut detail = mcx::vec_with_capacity_in(mcx, shared_jit_num_workers as usize)?;
    for n in 0..shared_jit_num_workers {
        let w = sup::shared_jit_instr::call(shared_jit, n);
        sup::accum_es_jit_worker_instr::call(estate, w);
        detail.push(w);
    }

    sup::set_worker_jit_instrument::call(planstate, shared_jit_num_workers, &detail);
    Ok(())
}

// ===========================================================================
// 14. ExecParallelFinish (execParallel.c:1155-1200)
// ===========================================================================

/// `ExecParallelFinish` — finish parallel execution: wait for workers,
/// accumulate buffer/WAL usage. No-op if called twice.
pub fn ExecParallelFinish<'mcx>(pei: &mut ParallelExecutorInfo<'mcx>) -> PgResult<()> {
    let pcxt = pei
        .pcxt
        .ok_or_else(|| PgError::error("ExecParallelFinish: pei->pcxt is live"))?;
    let nworkers = parallel::pcxt_nworkers_launched::call(pcxt);

    // Make this be a no-op if called twice in a row.
    if pei.finished {
        return Ok(());
    }

    // Detach from tuple queues ASAP, so that any still-active workers notice
    // that no further results are wanted.
    if !pei.tqueue.is_empty() {
        for i in 0..nworkers {
            shmmq::shm_mq_detach::call(pei.tqueue[i as usize]);
        }
        pei.tqueue.clear();
    }

    // While we're waiting for the workers to finish, get rid of the tuple queue
    // readers.
    if !pei.reader.is_empty() {
        for i in 0..nworkers {
            tqueue::destroy_tuple_queue_reader::call(pei.reader[i as usize]);
        }
        pei.reader.clear();
    }

    // Now wait for the workers to finish.
    parallel::wait_for_parallel_workers_to_finish::call(pcxt)?;

    // Next, accumulate buffer/WAL usage.
    for i in 0..nworkers {
        sup::instr_accum_parallel_query::call(pei.buffer_usage, pei.wal_usage, i);
    }

    pei.finished = true;
    Ok(())
}

// ===========================================================================
// 15. ExecParallelCleanup (execParallel.c:1208-1238)
// ===========================================================================

/// `ExecParallelCleanup` — accumulate instrumentation and clean up any
/// remaining [`ParallelExecutorInfo`] resources after [`ExecParallelFinish`].
pub fn ExecParallelCleanup<'mcx>(pei: &mut ParallelExecutorInfo<'mcx>) -> PgResult<()> {
    // The instrumentation retrieval allocates per-worker detail in the
    // per-query context; use a transient context for the build buffers.
    let cleanup_ctx = mcx::MemoryContext::new("ExecParallelCleanup");

    // Accumulate instrumentation, if any.
    if let Some(instrumentation) = pei.instrumentation {
        ExecParallelRetrieveInstrumentation(cleanup_ctx.mcx(), pei.planstate, instrumentation)?;
    }

    // Accumulate JIT instrumentation, if any.
    if let Some(jit_instrumentation) = pei.jit_instrumentation {
        ExecParallelRetrieveJitInstrumentation(cleanup_ctx.mcx(), pei.planstate, jit_instrumentation)?;
    }

    // Free any serialized parameters.
    if dsa_pointer_is_valid(pei.param_exec) {
        if let Some(area) = pei.area {
            dsa::dsa_free::call(area, pei.param_exec);
        }
        pei.param_exec = INVALID_DSA_POINTER;
    }
    if let Some(area) = pei.area.take() {
        dsa::dsa_detach::call(area);
    }
    if let Some(pcxt) = pei.pcxt.take() {
        parallel::destroy_parallel_context::call(pcxt)?;
    }
    // C `pfree(pei)`: the owned value is dropped by the caller.
    Ok(())
}

// ===========================================================================
// 16. ExecParallelGetReceiver (execParallel.c:1244-1255)
// ===========================================================================

/// `ExecParallelGetReceiver` — create a `DestReceiver` that writes tuples to the
/// shm_mq for this worker.
fn ExecParallelGetReceiver(
    seg: DsmSegmentHandle,
    toc: ShmTocHandle,
) -> PgResult<types_execparallel::DestReceiverHandle> {
    let mqspace = parallel::shm_toc_lookup::call(toc, PARALLEL_KEY_TUPLE_QUEUE, false)
        .ok_or_else(|| PgError::error("ExecParallelGetReceiver: PARALLEL_KEY_TUPLE_QUEUE present"))?;
    // mqspace += ParallelWorkerNumber * PARALLEL_TUPLE_QUEUE_SIZE
    let mq = shmmq::shm_mq_create_at::call(
        mqspace,
        parallel::parallel_worker_number::call(),
        PARALLEL_TUPLE_QUEUE_SIZE,
    );
    shmmq::shm_mq_set_sender_to_myproc::call(mq);
    Ok(tqueue::create_tuple_queue_dest_receiver::call(shmmq::shm_mq_attach::call(mq, Some(seg))?))
}

// ===========================================================================
// 17. ExecParallelGetQueryDesc (execParallel.c:1260-1286)
// ===========================================================================

/// `ExecParallelGetQueryDesc` — create a `QueryDesc` for the `PlannedStmt` we
/// are to execute.
fn ExecParallelGetQueryDesc(
    toc: ShmTocHandle,
    receiver: types_execparallel::DestReceiverHandle,
    instrument_options: i32,
) -> PgResult<QueryDescHandle> {
    // Get the query string from shared memory.
    let query_chunk = parallel::shm_toc_lookup::call(toc, PARALLEL_KEY_QUERY_TEXT, false)
        .ok_or_else(|| PgError::error("ExecParallelGetQueryDesc: PARALLEL_KEY_QUERY_TEXT present"))?;
    let query_string = parallel::cursor_cstring::call(query_chunk)?;

    // Reconstruct leader-supplied PlannedStmt.
    let pstmt_chunk = parallel::shm_toc_lookup::call(toc, PARALLEL_KEY_PLANNEDSTMT, false)
        .ok_or_else(|| PgError::error("ExecParallelGetQueryDesc: PARALLEL_KEY_PLANNEDSTMT present"))?;
    let pstmt = sup::string_to_plannedstmt::call(parallel::cursor_cstring::call(pstmt_chunk)?)?;

    // Reconstruct ParamListInfo.
    let param_chunk = parallel::shm_toc_lookup::call(toc, PARALLEL_KEY_PARAMLISTINFO, false)
        .ok_or_else(|| PgError::error("ExecParallelGetQueryDesc: PARALLEL_KEY_PARAMLISTINFO present"))?;
    let param_li = sup::restore_param_list::call(param_chunk);

    // Create a QueryDesc for the query (GetActiveSnapshot(), InvalidSnapshot).
    sup::create_query_desc::call(
        pstmt,
        query_string,
        Some(sup::get_active_snapshot::call()),
        None,
        receiver,
        param_li,
        instrument_options,
    )
}

// ===========================================================================
// 18. ExecParallelReportInstrumentation (execParallel.c:1292-1326)
// ===========================================================================

/// `ExecParallelReportInstrumentation` — copy instrumentation from this node and
/// its descendants into DSM so the leader can retrieve it. Runs in the worker.
fn ExecParallelReportInstrumentation(
    planstate: PlanStateHandle,
    instrumentation: InstrumentationHandle,
) -> PgResult<bool> {
    let plan_node_id = sup::plan_node_id::call(planstate);

    sup::instr_end_loop::call(planstate);

    // Find this node's slot (linear search, matching C).
    let num_plan_nodes = parallel::sei_num_plan_nodes::call(instrumentation);
    let num_workers = parallel::sei_num_workers::call(instrumentation);
    let mut i: i32 = 0;
    while i < num_plan_nodes {
        if parallel::sei_plan_node_id::call(instrumentation, i) == plan_node_id {
            break;
        }
        i += 1;
    }
    if i >= num_plan_nodes {
        return Err(PgError::error(format!("plan node {plan_node_id} not found"))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR));
    }

    // Add our statistics to the per-node, per-worker totals.
    let base = i * num_workers;
    let parallel_worker_number = parallel::parallel_worker_number::call();
    // Assert(IsParallelWorker()) == (ParallelWorkerNumber >= 0)
    debug_assert!(parallel_worker_number >= 0);
    debug_assert!(parallel_worker_number < num_workers);
    let slot = sup::sei_instrument_slots::call(instrumentation, base + parallel_worker_number, 1)
        .into_iter()
        .next()
        .unwrap_or_default();
    let aggregated = sup::instr_agg_node_value::call(slot, planstate);
    sup::sei_agg_into_slot::call(instrumentation, base + parallel_worker_number, aggregated);

    for child in sup::planstate_children::call(planstate)? {
        if ExecParallelReportInstrumentation(child, instrumentation)? {
            return Ok(true);
        }
    }
    Ok(false)
}

// ===========================================================================
// 19. ExecParallelInitializeWorker (execParallel.c:1333-1410)
// ===========================================================================

/// `ExecParallelInitializeWorker` — initialize the `PlanState` tree in the
/// worker with info from shared memory.
fn ExecParallelInitializeWorker(
    planstate: PlanStateHandle,
    pwcxt: ParallelWorkerContextHandle,
) -> PgResult<bool> {
    let tag = sup::node_tag::call(planstate);
    if tag == T_SeqScanState {
        if sup::plan_parallel_aware::call(planstate) {
            nodeSeqscan::exec_seqscan_initialize_worker::call(planstate, pwcxt)?;
        }
    } else if tag == T_IndexScanState {
        nodeIndex::exec_indexscan_initialize_worker::call(planstate, pwcxt)?;
    } else if tag == T_IndexOnlyScanState {
        nodeIndexOnly::exec_indexonlyscan_initialize_worker::call(planstate, pwcxt)?;
    } else if tag == T_BitmapIndexScanState {
        nodeBitmapIndex::exec_bitmapindexscan_initialize_worker::call(planstate, pwcxt)?;
    } else if tag == T_ForeignScanState {
        if sup::plan_parallel_aware::call(planstate) {
            nodeForeign::exec_foreignscan_initialize_worker::call(planstate, pwcxt)?;
        }
    } else if tag == T_AppendState {
        if sup::plan_parallel_aware::call(planstate) {
            nodeAppend::exec_append_initialize_worker::call(planstate, pwcxt)?;
        }
    } else if tag == T_CustomScanState {
        if sup::plan_parallel_aware::call(planstate) {
            nodeCustom::exec_customscan_initialize_worker::call(planstate, pwcxt)?;
        }
    } else if tag == T_BitmapHeapScanState {
        if sup::plan_parallel_aware::call(planstate) {
            nodeBitmapHeap::exec_bitmapheap_initialize_worker::call(planstate, pwcxt)?;
        }
    } else if tag == T_HashJoinState {
        if sup::plan_parallel_aware::call(planstate) {
            nodeHashjoin::exec_hashjoin_initialize_worker::call(planstate, pwcxt)?;
        }
    } else if tag == T_HashState {
        nodeHash::exec_hash_initialize_worker::call(planstate, pwcxt)?;
    } else if tag == T_SortState {
        nodeSort::exec_sort_initialize_worker::call(planstate, pwcxt)?;
    } else if tag == T_IncrementalSortState {
        nodeIncrSort::exec_incrementalsort_initialize_worker::call(planstate, pwcxt)?;
    } else if tag == T_AggState {
        nodeAgg::exec_agg_initialize_worker::call(planstate, pwcxt)?;
    } else if tag == T_MemoizeState {
        nodeMemoize::exec_memoize_initialize_worker::call(planstate, pwcxt)?;
    }

    for child in sup::planstate_children::call(planstate)? {
        if ExecParallelInitializeWorker(child, pwcxt)? {
            return Ok(true);
        }
    }
    Ok(false)
}

// ===========================================================================
// 20. ParallelQueryMain (execParallel.c:1428-1531)
// ===========================================================================

/// `ParallelQueryMain` — main entry point for parallel query worker processes.
/// Sets up the receiver/queryDesc, runs the plan, reports usage +
/// instrumentation, cleans up.
pub fn ParallelQueryMain<'mcx>(
    mcx: Mcx<'mcx>,
    seg: DsmSegmentHandle,
    toc: ShmTocHandle,
) -> PgResult<()> {
    let mut instrument_options: i32 = 0;

    // Get fixed-size state.
    let fpes_chunk = parallel::shm_toc_lookup::call(toc, PARALLEL_KEY_EXECUTOR_FIXED, false)
        .ok_or_else(|| PgError::error("ParallelQueryMain: PARALLEL_KEY_EXECUTOR_FIXED present"))?;
    let fpes = parallel::fixed_state_from_chunk::call(fpes_chunk);

    // Set up DestReceiver, SharedExecutorInstrumentation, and QueryDesc.
    let receiver = ExecParallelGetReceiver(seg, toc)?;
    let instrumentation = parallel::shm_toc_lookup::call(toc, PARALLEL_KEY_INSTRUMENTATION, true)
        .map(parallel::instrumentation_from_chunk::call);
    if let Some(sei) = instrumentation {
        instrument_options = parallel::sei_instrument_options::call(sei);
    }
    let jit_instrumentation = parallel::shm_toc_lookup::call(toc, PARALLEL_KEY_JIT_INSTRUMENTATION, true)
        .map(parallel::jit_instrumentation_from_chunk::call);
    let query_desc = ExecParallelGetQueryDesc(toc, receiver, instrument_options)?;

    // Setting debug_query_string for individual workers.
    sup::set_debug_query_string::call(sup::query_desc_source_text::call(query_desc)?);

    // Report workers' query for monitoring purposes.
    sup::pgstat_report_activity_running::call(sup::query_desc_source_text::call(query_desc)?);

    // Attach to the dynamic shared memory area.
    let area_chunk = parallel::shm_toc_lookup::call(toc, PARALLEL_KEY_DSA, false)
        .ok_or_else(|| PgError::error("ParallelQueryMain: PARALLEL_KEY_DSA present"))?;
    let area = dsa::dsa_attach_in_place::call(area_chunk, seg);

    // Start up the executor.
    sup::set_query_desc_jit_flags::call(query_desc, parallel::fixed_jit_flags::call(fpes));
    sup::executor_start::call(query_desc, parallel::fixed_eflags::call(fpes))?;

    // Special executor initialization steps for parallel workers.
    let query_desc_estate = sup::query_desc_estate::call(query_desc);
    let query_desc_planstate = sup::query_desc_planstate::call(query_desc);
    sup::set_es_query_dsa::call(query_desc_estate, Some(area));
    if dsa_pointer_is_valid(parallel::fixed_param_exec::call(fpes)) {
        let paramexec_cursor = dsa::dsa_get_address::call(area, parallel::fixed_param_exec::call(fpes));
        RestoreParamExecParams(paramexec_cursor, query_desc_estate);
    }
    let pwcxt = parallel::make_parallel_worker_context::call(seg, toc);
    ExecParallelInitializeWorker(query_desc_planstate, pwcxt)?;

    // Pass down any tuple bound.
    let tuples_needed = parallel::fixed_tuples_needed::call(fpes);
    sup::exec_set_tuple_bound::call(tuples_needed, query_desc_planstate)?;

    // Prepare to track buffer/WAL usage during query execution.
    sup::instr_start_parallel_query::call();

    // Run the plan. If we specified a tuple bound, be careful not to demand more
    // tuples than that.
    sup::executor_run::call(
        query_desc,
        FORWARD_SCAN_DIRECTION,
        if tuples_needed < 0 { 0 } else { tuples_needed },
    )?;

    // Shut down the executor.
    sup::executor_finish::call(query_desc)?;

    // Report buffer/WAL usage during parallel execution.
    let buffer_usage = parallel::shm_toc_lookup::call(toc, PARALLEL_KEY_BUFFER_USAGE, false)
        .ok_or_else(|| PgError::error("ParallelQueryMain: PARALLEL_KEY_BUFFER_USAGE present"))?;
    let wal_usage = parallel::shm_toc_lookup::call(toc, PARALLEL_KEY_WAL_USAGE, false)
        .ok_or_else(|| PgError::error("ParallelQueryMain: PARALLEL_KEY_WAL_USAGE present"))?;
    let parallel_worker_number = parallel::parallel_worker_number::call();
    sup::instr_end_parallel_query::call(buffer_usage, wal_usage, parallel_worker_number);

    // Report instrumentation data if any instrumentation options are set.
    if let Some(sei) = instrumentation {
        ExecParallelReportInstrumentation(query_desc_planstate, sei)?;
    }

    // Report JIT instrumentation data if any.
    if sup::es_has_jit::call(query_desc_estate) {
        if let Some(jit) = jit_instrumentation {
            debug_assert!(parallel_worker_number < parallel::shared_jit_num_workers::call(jit));
            sup::set_shared_jit_instr::call(
                jit,
                parallel_worker_number,
                sup::es_jit_instr::call(query_desc_estate),
            );
        }
    }

    // Must do this after capturing instrumentation.
    sup::executor_end::call(query_desc)?;

    // Cleanup.
    dsa::dsa_detach::call(area);
    sup::free_query_desc::call(query_desc);
    tqueue::receiver_destroy::call(receiver);
    let _ = mcx;
    Ok(())
}
