//! Port of `execParallel.c` — the executor's parallel-query infrastructure
//! (PostgreSQL 18.3).
//!
//! Sets up, uses, and tears down a `ParallelContext` from within the executor:
//! serializes the plan + parameters into a DSM segment, walks the `PlanState`
//! tree to size and populate that segment, sets up the tuple queues the workers
//! write into, launches/reaps the workers, and aggregates their buffer/WAL
//! usage and instrumentation back into the leader.
//!
//! The plan-state-tree walks (the `planstate_tree_walker` dispatch of the
//! per-node parallel methods) are this crate's own logic and are driven over the
//! executor's **owned** `PlanStateNode` tree (`&mut PlanStateNode<'mcx>` +
//! `&mut EStateData<'mcx>`) — an enum match on the concrete node, exactly like
//! `execProcnode`'s `ExecEndNode`. The per-node `Exec*Estimate` /
//! `*InitializeDSM` / `*InitializeWorker` / `*ReInitializeDSM` methods are
//! called directly on the owned node-state structs.
//!
//! What still crosses a seam: the DSM / `shm_toc` / `shm_mq` primitives, the
//! `ParallelContext` machinery, the tuple-queue reader/writer, the DSA area —
//! all the dynamic-shared-memory carrier — plus the worker plan-shipping
//! (`nodeToString` / `SerializeParamList` and their restore counterparts),
//! parameter/datum (de)serialization, snapshot management, and `pgstat`
//! reporting. The executor driver (`ExecutorStart`/`Run`/`Finish`/`End`) and the
//! `QueryDesc` lifecycle are owned (`execMain` / `types_nodes::QueryDesc`) and
//! called directly.
//!
//! Parallel-DSM-carrier residual (sanctioned mirror-and-panic): the per-node
//! parallel methods that read/write DSM-resident shared state whose typed
//! shared-DSM-object carrier has not landed (the `Exec*InitializeDSM`/`Worker`
//! *bodies* for Seq/Index/Hash/Sort/Append/… honestly panic where they reach
//! the unbuilt carrier), the `Foreign`/`Custom` parallel methods (no bridge from
//! the DSM-owned `ParallelContextHandle` to the owned `&mut ParallelContext`
//! they take), the `IncrementalSort` parallel methods (no owned node crate yet),
//! and the per-`PlanState` instrumentation retrieval/report walks (the leader's
//! `planstate->worker_instrument` / worker's `SharedExecutorInstrumentation`
//! accumulation — the owned `PlanState` head does not yet carry the
//! `worker_instrument` array). Those points panic loudly with their rationale.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use mcx::{Mcx, PgVec};
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR, ERRCODE_PROGRAM_LIMIT_EXCEEDED};
use types_execparallel::{
    dsa_pointer_is_valid, DsaAreaHandle, DsmSegmentHandle, FixedParallelExecutorState,
    ParallelExecutorInfo, SerializeCursor, SharedExecutorInstrumentation, ShmTocHandle, Size,
    TuplesNeeded, DsaPointer, INVALID_DSA_POINTER, PGJIT_NONE,
};
use types_nodes::bitmapset::Bitmapset;
use types_nodes::querydesc::QueryDesc;
use types_nodes::{EStateData, PlanStateNode};
use types_core::instrument::{BufferUsage, WalUsage};
use types_nodes::instrument::Instrumentation;

use backend_access_transam_parallel_seams as parallel;
use backend_executor_execParallel_support_seams as sup;
use backend_executor_tqueue_seams as tqueue;
use backend_storage_ipc_shm_mq_seams as shmmq;
use backend_utils_mmgr_dsa_seams as dsa;

use backend_executor_execMain as execMain;

use backend_executor_nodeAppend as nodeAppend;
use backend_executor_nodeBitmapHeapscan as nodeBitmapHeap;
use backend_executor_nodeBitmapIndexscan as nodeBitmapIndex;
use backend_executor_nodeHash as nodeHash;
use backend_executor_nodeHashjoin as nodeHashjoin;
use backend_executor_nodeIndexonlyscan as nodeIndexOnly;
use backend_executor_nodeIndexscan as nodeIndex;
use backend_executor_nodeMemoize as nodeMemoize;
use backend_executor_nodeSeqscan as nodeSeqscan;
use backend_executor_nodeSort as nodeSort;

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
const FORWARD_SCAN_DIRECTION: types_scan::sdir::ScanDirection =
    types_scan::sdir::ScanDirection::ForwardScanDirection;

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
    backend_executor_execParallel_seams::ExecParallelCreateReaders::set(ExecParallelCreateReaders);
    backend_executor_execParallel_seams::ExecParallelFinish::set(ExecParallelFinish);
    backend_executor_execParallel_seams::ParallelQueryMain::set(ParallelQueryMain);
    // The de-handled owned entry points consumed by nodeGather/nodeGatherMerge.
    backend_executor_execParallel_seams::exec_init_parallel_plan_owned::set(
        ExecInitParallelPlan,
    );
    backend_executor_execParallel_seams::exec_parallel_reinitialize_owned::set(
        ExecParallelReinitialize,
    );
    backend_executor_execParallel_seams::ExecParallelCleanup::set(ExecParallelCleanup);
}

// ===========================================================================
// 1. ExecSerializePlan (execParallel.c:145-221)
// ===========================================================================

/// `ExecSerializePlan` — create a serialized representation of the plan to be
/// sent to each worker. Returns `nodeToString(pstmt)`.
///
/// The plan-fix-up + serialization is the worker plan-shipping path, owned by
/// `copyfuncs.c`/`outfuncs.c` (not yet ported); the owned plan tree
/// (`estate->es_plannedstmt` reached through the owned `EStateData`) is handed
/// to the plan-shipping seam, which honestly panics until those land.
fn ExecSerializePlan(estate: &mut EStateData<'_>) -> PgResult<String> {
    // We can't scribble on the original plan, so make a copy; clear resjunk on
    // the top target list; build the dummy PlannedStmt (field-fill +
    // parallel-safe-subplan filtering) and serialize it. The whole plan-shipping
    // pipeline (copyObject(plan) → clear resjunk → build serializable
    // PlannedStmt → nodeToString) is the worker plan-shipping path; reached
    // through the owner seam, which panics until copyfuncs/out funcs land.
    sup::serialize_plan_for_workers::call(estate)
}

// ===========================================================================
// 2. ExecParallelEstimate (execParallel.c:232-313)
// ===========================================================================

/// `ExecParallelEstimate` — per-node DSM-estimate tree walk. Counts the node,
/// dispatches the node's `ExecXxxEstimate` over the owned node-state enum, then
/// recurses over the `planstate_tree_walker` children.
fn ExecParallelEstimate<'mcx>(
    planstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
    pcxt: types_execparallel::ParallelContextHandle,
    nnodes: &mut i32,
) -> PgResult<bool> {
    // Count this node.
    *nnodes += 1;

    let parallel_aware = planstate.parallel_aware();
    match planstate {
        // case T_SeqScanState: if (planstate->plan->parallel_aware)
        //     ExecSeqScanEstimate((SeqScanState *) planstate, e->pcxt);
        PlanStateNode::SeqScan(node) => {
            if parallel_aware {
                nodeSeqscan::ExecSeqScanEstimate(node, pcxt, estate)?;
            }
        }
        // case T_IndexScanState: ExecIndexScanEstimate(...);
        PlanStateNode::IndexScan(node) => {
            nodeIndex::ExecIndexScanEstimate(node, pcxt, estate)?;
        }
        // case T_IndexOnlyScanState: ExecIndexOnlyScanEstimate(...);
        PlanStateNode::IndexOnlyScan(node) => {
            nodeIndexOnly::ExecIndexOnlyScanEstimate(node, pcxt, estate)?;
        }
        // case T_BitmapIndexScanState: ExecBitmapIndexScanEstimate(...);
        PlanStateNode::BitmapIndexScan(node) => {
            nodeBitmapIndex::ExecBitmapIndexScanEstimate(node, pcxt)?;
        }
        // case T_ForeignScanState: if (parallel_aware) ExecForeignScanEstimate(..);
        PlanStateNode::ForeignScan(node) => {
            if parallel_aware {
                foreignscan_no_owned_pcxt("ExecForeignScanEstimate", node);
            }
        }
        // case T_AppendState: if (parallel_aware) ExecAppendEstimate(..);
        PlanStateNode::Append(node) => {
            if parallel_aware {
                nodeAppend::ExecAppendEstimate(node, pcxt)?;
            }
        }
        // case T_CustomScanState: if (parallel_aware) ExecCustomScanEstimate(..);
        PlanStateNode::CustomScan(node) => {
            if parallel_aware {
                customscan_no_owned_pcxt("ExecCustomScanEstimate", node);
            }
        }
        // case T_BitmapHeapScanState: if (parallel_aware) ExecBitmapHeapEstimate(..);
        PlanStateNode::BitmapHeapScan(node) => {
            if parallel_aware {
                nodeBitmapHeap::ExecBitmapHeapEstimate(node, pcxt)?;
            }
        }
        // case T_HashJoinState: if (parallel_aware) ExecHashJoinEstimate(..);
        PlanStateNode::HashJoin(node) => {
            if parallel_aware {
                nodeHashjoin::ExecHashJoinEstimate(node, pcxt)?;
            }
        }
        // case T_HashState: ExecHashEstimate(..);
        PlanStateNode::Hash(node) => {
            nodeHash::instrument::ExecHashEstimate(node, pcxt)?;
        }
        // case T_SortState: ExecSortEstimate(..);
        PlanStateNode::Sort(node) => {
            nodeSort::ExecSortEstimate(node, pcxt)?;
        }
        // case T_MemoizeState: ExecMemoizeEstimate(..);
        PlanStateNode::Memoize(node) => {
            nodeMemoize::ExecMemoizeEstimate(node, pcxt)?;
        }
        // case T_IncrementalSortState: ExecIncrementalSortEstimate(..); and
        // case T_AggState: ExecAggEstimate(..) — the IncrementalSortState and
        // AggState variants are not present in the `#[non_exhaustive]`
        // `PlanStateNode` enum yet (no owned nodeIncrementalSort crate; nodeAgg
        // does not thread its AggState into the enum), so those tags cannot
        // occur. They add their arm here as those units thread their state in.
        // No DSM-estimate method for any other node tag (C `default: break`).
        _ => {}
    }

    // return planstate_tree_walker(planstate, ExecParallelEstimate, e);
    for child in planstate.planstate_tree_walker_children_mut() {
        if ExecParallelEstimate(child, estate, pcxt, nnodes)? {
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
fn EstimateParamExecSpace(estate: &mut EStateData<'_>, params: &Bitmapset) -> PgResult<Size> {
    let mut sz: Size = core::mem::size_of::<i32>();

    let mut paramid: i32 = -1;
    loop {
        paramid = sup::bms_next_member::call(params, paramid);
        if paramid < 0 {
            break;
        }

        // value/isnull + resolved (typByVal, typLen): the C `get_typlenbyval` /
        // no-OID-by-value fallback folded into one read out of the owned EState.
        let prm = sup::param_exec_value_owned::call(estate, paramid);

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
    estate: &mut EStateData<'_>,
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

        let prm = sup::param_exec_value_owned::call(estate, paramid);

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
fn RestoreParamExecParams(cursor: SerializeCursor, estate: &mut EStateData<'_>) {
    let (nparams, mut cursor) = sup::datum_restore_i32::call(cursor);

    for _ in 0..nparams {
        let (paramid, next) = sup::datum_restore_i32::call(cursor);
        cursor = next;

        let (restored, next) = sup::datum_restore::call(cursor);
        cursor = next;

        // prm->value/isnull = ...; prm->execPlan = NULL;
        sup::set_param_exec_value_owned::call(estate, paramid, restored);
    }
}

// ===========================================================================
// 6. ExecParallelInitializeDSM (execParallel.c:446-540)
// ===========================================================================

/// `ExecParallelInitializeDSM` — per-node DSM-initialize tree walk over the
/// owned node-state enum.
fn ExecParallelInitializeDSM<'mcx>(
    planstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
    pcxt: types_execparallel::ParallelContextHandle,
    instrumentation: Option<types_execparallel::InstrumentationHandle>,
    nnodes: &mut i32,
) -> PgResult<bool> {
    // If instrumentation is enabled, initialize slot for this node.
    if let Some(sei) = instrumentation {
        parallel::set_sei_plan_node_id::call(sei, *nnodes, planstate.plan_node_id());
    }

    // Count this node.
    *nnodes += 1;

    let parallel_aware = planstate.parallel_aware();
    match planstate {
        PlanStateNode::SeqScan(node) => {
            if parallel_aware {
                nodeSeqscan::ExecSeqScanInitializeDSM(node, pcxt, estate)?;
            }
        }
        PlanStateNode::IndexScan(node) => {
            nodeIndex::ExecIndexScanInitializeDSM(node, pcxt, estate)?;
        }
        PlanStateNode::IndexOnlyScan(node) => {
            nodeIndexOnly::ExecIndexOnlyScanInitializeDSM(node, pcxt, estate)?;
        }
        PlanStateNode::BitmapIndexScan(node) => {
            nodeBitmapIndex::ExecBitmapIndexScanInitializeDSM(node, pcxt, estate)?;
        }
        PlanStateNode::ForeignScan(node) => {
            if parallel_aware {
                foreignscan_no_owned_pcxt("ExecForeignScanInitializeDSM", node);
            }
        }
        PlanStateNode::Append(node) => {
            if parallel_aware {
                nodeAppend::ExecAppendInitializeDSM(node, pcxt)?;
            }
        }
        PlanStateNode::CustomScan(node) => {
            if parallel_aware {
                customscan_no_owned_pcxt("ExecCustomScanInitializeDSM", node);
            }
        }
        PlanStateNode::BitmapHeapScan(node) => {
            if parallel_aware {
                nodeBitmapHeap::ExecBitmapHeapInitializeDSM(node, pcxt, estate)?;
            }
        }
        PlanStateNode::HashJoin(node) => {
            if parallel_aware {
                nodeHashjoin::ExecHashJoinInitializeDSM(node, pcxt)?;
            }
        }
        PlanStateNode::Hash(node) => {
            nodeHash::instrument::ExecHashInitializeDSM(node, pcxt)?;
        }
        PlanStateNode::Sort(node) => {
            nodeSort::ExecSortInitializeDSM(node, pcxt)?;
        }
        PlanStateNode::Memoize(node) => {
            nodeMemoize::ExecMemoizeInitializeDSM(node, pcxt)?;
        }
        _ => {}
    }

    for child in planstate.planstate_tree_walker_children_mut() {
        if ExecParallelInitializeDSM(child, estate, pcxt, instrumentation, nnodes)? {
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
    pcxt: types_execparallel::ParallelContextHandle,
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
///
/// Owned form (`exec_init_parallel_plan_owned`): `planstate` is the leader's
/// `outerPlanState` and `estate` the per-query `EState`, both threaded by `&mut`
/// (the executor owns the tree), exactly as C threads the two pointers.
pub fn ExecInitParallelPlan<'mcx>(
    mcx: Mcx<'mcx>,
    planstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
    send_params: Option<&Bitmapset>,
    nworkers: i32,
    tuples_needed: TuplesNeeded,
) -> PgResult<ParallelExecutorInfo<'mcx>> {
    let empty_bms;
    let send_params: &Bitmapset = match send_params {
        Some(b) => b,
        None => {
            empty_bms = Bitmapset { words: PgVec::new_in(mcx) };
            &empty_bms
        }
    };

    let mut instrumentation_len: i32 = 0;
    let mut jit_instrumentation_len: i32 = 0;
    let mut instrument_offset: i32 = 0;
    let dsa_minsize: Size = dsa::dsa_minimum_size::call();

    // Force any initplan outputs to be evaluated, if they weren't already.
    let per_tuple_econtext = sup::get_per_tuple_expr_context_owned::call(estate)?;
    sup::exec_set_param_plan_multi::call(send_params, per_tuple_econtext)?;

    // Allocate object for return value (palloc0 defaults).
    let mut pei = ParallelExecutorInfo {
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
    let pstmt_data = ExecSerializePlan(estate)?;

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
    let query_text = estate
        .es_sourceText
        .as_ref()
        .map(|s| s.as_str().to_string())
        .ok_or_else(|| PgError::error("ExecInitParallelPlan: estate->es_sourceText is NULL"))?;
    let query_len: i32 = query_text.len() as i32;
    parallel::shm_toc_estimate_chunk::call(estimator, query_len as Size + 1);
    parallel::shm_toc_estimate_keys::call(estimator, 1);

    // Estimate space for serialized PlannedStmt.
    let pstmt_len: i32 = pstmt_data.len() as i32 + 1;
    parallel::shm_toc_estimate_chunk::call(estimator, pstmt_len as Size);
    parallel::shm_toc_estimate_keys::call(estimator, 1);

    // Estimate space for serialized ParamListInfo.
    let param_li = estate.es_param_list_info;
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
    let mut nnodes_estimate: i32 = 0;
    ExecParallelEstimate(planstate, estate, pcxt, &mut nnodes_estimate)?;

    // Estimate space for instrumentation, if required.
    let es_instrument = estate.es_instrument;
    let es_jit_flags = estate.es_jit_flags;
    if es_instrument != 0 {
        instrumentation_len =
            OFFSET_OF_PLAN_NODE_ID as i32 + core::mem::size_of::<i32>() as i32 * nnodes_estimate;
        instrumentation_len = maxalign(instrumentation_len as usize) as i32;
        instrument_offset = instrumentation_len;
        instrumentation_len += mul_size(
            SIZEOF_INSTRUMENTATION,
            mul_size(nnodes_estimate as usize, nworkers as usize)?,
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
            eflags: estate.es_top_eflags,
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
    sup::serialize_param_list::call(param_li, paramlistinfo_chunk)?;

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
                num_plan_nodes: nnodes_estimate,
            },
        );
        for i in 0..(nworkers * nnodes_estimate) {
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
    let instrumentation = pei.instrumentation;
    let mut nnodes_init: i32 = 0;

    // Install our DSA area while initializing the plan.
    estate.es_query_dsa = pei.area;
    ExecParallelInitializeDSM(planstate, estate, pcxt, instrumentation, &mut nnodes_init)?;
    estate.es_query_dsa = None;

    // Make sure that the world hasn't shifted under our feet.
    if nnodes_estimate != nnodes_init {
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
    planstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
    pei: &mut ParallelExecutorInfo<'mcx>,
    send_params: Option<&Bitmapset>,
) -> PgResult<()> {
    let empty_bms;
    let send_params: &Bitmapset = match send_params {
        Some(b) => b,
        None => {
            empty_bms = Bitmapset { words: PgVec::new_in(mcx) };
            &empty_bms
        }
    };

    // Old workers must already be shut down.
    debug_assert!(pei.finished);

    let pcxt = pei
        .pcxt
        .ok_or_else(|| PgError::error("ExecParallelReinitialize: pei->pcxt is live"))?;

    // Force any initplan outputs to be evaluated, if they weren't already.
    //   EState *estate = planstate->state;  (threaded in by the caller)
    let per_tuple_econtext = sup::get_per_tuple_expr_context_owned::call(estate)?;
    sup::exec_set_param_plan_multi::call(send_params, per_tuple_econtext)?;

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
    estate.es_query_dsa = pei.area;
    ExecParallelReInitializeDSM(planstate, estate, pcxt)?;
    estate.es_query_dsa = None;
    Ok(())
}

// ===========================================================================
// 11. ExecParallelReInitializeDSM (execParallel.c:964-1028)
// ===========================================================================

/// `ExecParallelReInitializeDSM` — per-node DSM-reinitialize tree walk over the
/// owned node-state enum.
fn ExecParallelReInitializeDSM<'mcx>(
    planstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
    pcxt: types_execparallel::ParallelContextHandle,
) -> PgResult<bool> {
    let parallel_aware = planstate.parallel_aware();
    match planstate {
        PlanStateNode::SeqScan(node) => {
            if parallel_aware {
                nodeSeqscan::ExecSeqScanReInitializeDSM(node, pcxt)?;
            }
        }
        PlanStateNode::IndexScan(node) => {
            if parallel_aware {
                nodeIndex::ExecIndexScanReInitializeDSM(node, pcxt, estate)?;
            }
        }
        PlanStateNode::IndexOnlyScan(node) => {
            if parallel_aware {
                nodeIndexOnly::ExecIndexOnlyScanReInitializeDSM(node, pcxt, estate)?;
            }
        }
        PlanStateNode::ForeignScan(node) => {
            if parallel_aware {
                foreignscan_no_owned_pcxt("ExecForeignScanReInitializeDSM", node);
            }
        }
        PlanStateNode::Append(node) => {
            if parallel_aware {
                nodeAppend::ExecAppendReInitializeDSM(node, pcxt)?;
            }
        }
        PlanStateNode::CustomScan(node) => {
            if parallel_aware {
                customscan_no_owned_pcxt("ExecCustomScanReInitializeDSM", node);
            }
        }
        PlanStateNode::BitmapHeapScan(node) => {
            if parallel_aware {
                nodeBitmapHeap::ExecBitmapHeapReInitializeDSM(node, pcxt, estate)?;
            }
        }
        PlanStateNode::HashJoin(node) => {
            if parallel_aware {
                nodeHashjoin::ExecHashJoinReInitializeDSM(node, pcxt)?;
            }
        }
        // these nodes have DSM state, but no reinitialization is required:
        // T_BitmapIndexScanState / T_HashState / T_SortState /
        // T_IncrementalSortState / T_MemoizeState.
        PlanStateNode::BitmapIndexScan(_)
        | PlanStateNode::Hash(_)
        | PlanStateNode::Sort(_)
        | PlanStateNode::Memoize(_) => {}
        _ => {}
    }

    for child in planstate.planstate_tree_walker_children_mut() {
        if ExecParallelReInitializeDSM(child, estate, pcxt)? {
            return Ok(true);
        }
    }
    Ok(false)
}

// ===========================================================================
// 12. ExecParallelRetrieveInstrumentation (execParallel.c:1034-1110)
// 13. ExecParallelRetrieveJitInstrumentation (execParallel.c:1115-1149)
// — parallel-DSM-carrier residual (see crate docs). The per-`PlanState`
//   `worker_instrument` / `worker_jit_instrument` arrays are not yet modeled on
//   the owned `PlanState` head, and the generic per-node slot accumulation
//   reads/writes the DSM-resident `SharedExecutorInstrumentation`. Honest
//   panic until that carrier lands.
// ===========================================================================

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
///
/// `planstate` (the leader's `outerPlanState`) is threaded in by `&mut` rather
/// than carried inside the `pei` struct: in the owned model `pei` lives inside
/// the Gather/GatherMerge node-state and the planstate is a *sibling field*
/// (`node.pei` and `node.ps.lefttree`), so storing a self-reference in `pei`
/// would be a self-borrow. The caller hands both disjoint field borrows.
pub fn ExecParallelCleanup<'mcx>(
    pei: &mut ParallelExecutorInfo<'mcx>,
    planstate: &mut PlanStateNode<'mcx>,
) -> PgResult<()> {
    // Accumulate instrumentation, if any. (Parallel-DSM-carrier residual: the
    // leader's per-PlanState `worker_instrument`/`worker_jit_instrument` arrays
    // are not yet modeled on the owned PlanState head, and the per-node slot
    // accumulation reads the DSM-resident SharedExecutorInstrumentation; honest
    // panic until that carrier lands.)
    if pei.instrumentation.is_some() {
        let _ = &planstate;
        panic!(
            "ExecParallelRetrieveInstrumentation: the leader-side per-PlanState \
             worker_instrument accumulation from the DSM SharedExecutorInstrumentation \
             is not yet modeled (owned PlanState head carries no worker_instrument array; \
             parallel-DSM-carrier keystone pending)"
        );
    }
    if pei.jit_instrumentation.is_some() {
        panic!(
            "ExecParallelRetrieveJitInstrumentation: the leader-side per-PlanState \
             worker_jit_instrument accumulation from the DSM SharedJitInstrumentation \
             is not yet modeled (parallel-DSM-carrier keystone pending)"
        );
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
    // C: mq = (shm_mq *) mqspace — the worker *casts* the leader-created queue,
    // it does not re-create it (that would wipe the leader's mq_set_receiver).
    let mq = shmmq::shm_mq_at::call(
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
fn ExecParallelGetQueryDesc<'mcx>(
    mcx: Mcx<'mcx>,
    toc: ShmTocHandle,
    receiver: types_execparallel::DestReceiverHandle,
    instrument_options: i32,
) -> PgResult<QueryDesc> {
    // Get the query string from shared memory.
    let query_chunk = parallel::shm_toc_lookup::call(toc, PARALLEL_KEY_QUERY_TEXT, false)
        .ok_or_else(|| PgError::error("ExecParallelGetQueryDesc: PARALLEL_KEY_QUERY_TEXT present"))?;
    let query_string = parallel::cursor_cstring::call(query_chunk)?;

    // Reconstruct leader-supplied PlannedStmt.
    let pstmt_chunk = parallel::shm_toc_lookup::call(toc, PARALLEL_KEY_PLANNEDSTMT, false)
        .ok_or_else(|| PgError::error("ExecParallelGetQueryDesc: PARALLEL_KEY_PLANNEDSTMT present"))?;
    let pstmt = parallel::cursor_cstring::call(pstmt_chunk)?;

    // Reconstruct ParamListInfo.
    let param_chunk = parallel::shm_toc_lookup::call(toc, PARALLEL_KEY_PARAMLISTINFO, false)
        .ok_or_else(|| PgError::error("ExecParallelGetQueryDesc: PARALLEL_KEY_PARAMLISTINFO present"))?;
    let param_li = sup::restore_param_list::call(param_chunk);

    // Create a QueryDesc for the query (stringToNode(pstmt) + CreateQueryDesc
    // with GetActiveSnapshot()/InvalidSnapshot). The plan reconstruction
    // (`stringToNode`) is the worker plan-shipping path, owned by readfuncs.c;
    // reached through the owner seam, which returns the owned `QueryDesc` (or
    // honestly panics until plan-shipping lands).
    sup::create_parallel_query_desc::call(
        mcx,
        pstmt,
        query_string,
        receiver,
        param_li,
        instrument_options,
    )
}

// ===========================================================================
// 18. ExecParallelReportInstrumentation (execParallel.c:1292-1326)
// — parallel-DSM-carrier residual (see crate docs); honest panic at the call
//   site in ParallelQueryMain.
// ===========================================================================

// ===========================================================================
// 19. ExecParallelInitializeWorker (execParallel.c:1333-1410)
// ===========================================================================

/// `ExecParallelInitializeWorker` — initialize the `PlanState` tree in the
/// worker with info from shared memory, over the owned node-state enum.
fn ExecParallelInitializeWorker<'mcx>(
    planstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
    pwcxt: types_execparallel::ParallelWorkerContextHandle,
) -> PgResult<bool> {
    let parallel_aware = planstate.parallel_aware();
    match planstate {
        PlanStateNode::SeqScan(node) => {
            if parallel_aware {
                nodeSeqscan::ExecSeqScanInitializeWorker(node, pwcxt, estate)?;
            }
        }
        PlanStateNode::IndexScan(node) => {
            nodeIndex::ExecIndexScanInitializeWorker(node, pwcxt, estate)?;
        }
        PlanStateNode::IndexOnlyScan(node) => {
            nodeIndexOnly::ExecIndexOnlyScanInitializeWorker(node, pwcxt, estate)?;
        }
        PlanStateNode::BitmapIndexScan(node) => {
            nodeBitmapIndex::ExecBitmapIndexScanInitializeWorker(node, pwcxt, estate)?;
        }
        PlanStateNode::ForeignScan(node) => {
            if parallel_aware {
                foreignscan_no_owned_pcxt("ExecForeignScanInitializeWorker", node);
            }
        }
        PlanStateNode::Append(node) => {
            if parallel_aware {
                nodeAppend::ExecAppendInitializeWorker(node, pwcxt)?;
            }
        }
        PlanStateNode::CustomScan(node) => {
            if parallel_aware {
                customscan_no_owned_pcxt("ExecCustomScanInitializeWorker", node);
            }
        }
        PlanStateNode::BitmapHeapScan(node) => {
            if parallel_aware {
                nodeBitmapHeap::ExecBitmapHeapInitializeWorker(node, pwcxt, estate)?;
            }
        }
        PlanStateNode::HashJoin(node) => {
            if parallel_aware {
                nodeHashjoin::ExecHashJoinInitializeWorker(node, pwcxt)?;
            }
        }
        PlanStateNode::Hash(node) => {
            nodeHash::instrument::ExecHashInitializeWorker(node, pwcxt)?;
        }
        PlanStateNode::Sort(node) => {
            nodeSort::ExecSortInitializeWorker(node, pwcxt)?;
        }
        PlanStateNode::Memoize(node) => {
            nodeMemoize::ExecMemoizeInitializeWorker(node, pwcxt)?;
        }
        _ => {}
    }

    for child in planstate.planstate_tree_walker_children_mut() {
        if ExecParallelInitializeWorker(child, estate, pwcxt)? {
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
/// instrumentation, cleans up. Drives the owned `QueryDesc` + executor driver.
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
    let mut query_desc = ExecParallelGetQueryDesc(mcx, toc, receiver, instrument_options)?;

    // Setting debug_query_string for individual workers.
    let source_text = sup::query_desc_source_text_owned::call(&query_desc)?;
    sup::set_debug_query_string::call(source_text.clone());

    // Report workers' query for monitoring purposes.
    sup::pgstat_report_activity_running::call(source_text);

    // Attach to the dynamic shared memory area.
    let area_chunk = parallel::shm_toc_lookup::call(toc, PARALLEL_KEY_DSA, false)
        .ok_or_else(|| PgError::error("ParallelQueryMain: PARALLEL_KEY_DSA present"))?;
    let area = dsa::dsa_attach_in_place::call(area_chunk, seg);

    // Start up the executor.
    sup::set_query_desc_jit_flags_owned::call(&mut query_desc, parallel::fixed_jit_flags::call(fpes));
    execMain::ExecutorStart(&mut query_desc, parallel::fixed_eflags::call(fpes))?;

    // Special executor initialization steps for parallel workers:
    //   estate->es_query_dsa = area;
    //   if (DsaPointerIsValid(fpes->param_exec))
    //       RestoreParamExecParams(start_address, queryDesc->estate);
    //   ExecParallelInitializeWorker(queryDesc->planstate, &pwcxt);
    let pwcxt = parallel::make_parallel_worker_context::call(seg, toc);
    let fixed_param_exec = parallel::fixed_param_exec::call(fpes);
    let paramexec_cursor = if dsa_pointer_is_valid(fixed_param_exec) {
        Some(dsa::dsa_get_address::call(area, fixed_param_exec))
    } else {
        None
    };
    let tuples_needed = parallel::fixed_tuples_needed::call(fpes);

    query_desc.with_plan_and_estate_mut(|_plan, _pstmt, estate, planstate_slot| -> PgResult<()> {
        estate.es_query_dsa = Some(area);
        if let Some(cursor) = paramexec_cursor {
            RestoreParamExecParams(cursor, estate);
        }
        let planstate = planstate_slot
            .as_deref_mut()
            .ok_or_else(|| PgError::error("ParallelQueryMain: queryDesc->planstate is NULL"))?;
        ExecParallelInitializeWorker(planstate, estate, pwcxt)?;

        // Pass down any tuple bound.
        backend_executor_execProcnode::execProcnode_run_end::exec_set_tuple_bound(
            tuples_needed,
            planstate,
            estate,
        )?;
        Ok(())
    })?;

    // Prepare to track buffer/WAL usage during query execution.
    sup::instr_start_parallel_query::call();

    // Run the plan. If we specified a tuple bound, be careful not to demand more
    // tuples than that.
    let count: u64 = if tuples_needed < 0 { 0 } else { tuples_needed as u64 };
    execMain::ExecutorRun(&mut query_desc, FORWARD_SCAN_DIRECTION, count)?;

    // Shut down the executor.
    execMain::ExecutorFinish(&mut query_desc)?;

    // Report buffer/WAL usage during parallel execution.
    let buffer_usage = parallel::shm_toc_lookup::call(toc, PARALLEL_KEY_BUFFER_USAGE, false)
        .ok_or_else(|| PgError::error("ParallelQueryMain: PARALLEL_KEY_BUFFER_USAGE present"))?;
    let wal_usage = parallel::shm_toc_lookup::call(toc, PARALLEL_KEY_WAL_USAGE, false)
        .ok_or_else(|| PgError::error("ParallelQueryMain: PARALLEL_KEY_WAL_USAGE present"))?;
    let parallel_worker_number = parallel::parallel_worker_number::call();
    sup::instr_end_parallel_query::call(buffer_usage, wal_usage, parallel_worker_number);

    // Report instrumentation data if any instrumentation options are set.
    // (Parallel-DSM-carrier residual — the worker-side per-PlanState slot
    // accumulation into the DSM SharedExecutorInstrumentation is not yet
    // modeled; honest panic when instrumentation is present.)
    if instrumentation.is_some() {
        panic!(
            "ExecParallelReportInstrumentation: the worker-side per-PlanState slot \
             accumulation into the DSM SharedExecutorInstrumentation is not yet modeled \
             (parallel-DSM-carrier keystone pending)"
        );
    }

    // Report JIT instrumentation data if any.
    if sup::query_desc_estate_has_jit_owned::call(&query_desc) {
        if jit_instrumentation.is_some() {
            panic!(
                "ParallelQueryMain: worker JIT instrumentation report into the DSM \
                 SharedJitInstrumentation is not yet modeled (parallel-DSM-carrier \
                 keystone pending)"
            );
        }
    }

    // Must do this after capturing instrumentation.
    execMain::ExecutorEnd(&mut query_desc)?;

    // Cleanup.
    dsa::dsa_detach::call(area);
    // C `FreeQueryDesc(queryDesc)` — the owned `QueryDesc` is dropped here.
    drop(query_desc);
    tqueue::receiver_destroy::call(receiver);
    Ok(())
}

// ===========================================================================
// Foreign/Custom parallel-method dispatch: no owned-ParallelContext bridge.
//
// `ExecForeignScan*`/`ExecCustomScan*` take an owned `&mut ParallelContext`
// (nodeForeigncustom's owned type), but the parallel-executor tree walk holds a
// `ParallelContextHandle` — the DSM-owned coordination object reached through
// `backend-access-transam-parallel-seams`. No bridge from that handle to the
// owned `ParallelContext` value exists (the owned `ParallelContext` carries the
// `shm_toc` opaquely and has no producer from the handle), so the FDW /
// custom-scan parallel methods cannot be invoked from here yet. Honest panic
// with the rationale, mirroring the C dispatch reaching these arms.
// ===========================================================================

fn foreignscan_no_owned_pcxt(
    which: &str,
    _node: &mut mcx::PgBox<'_, types_nodes::nodeforeigncustom::ForeignScanState<'_>>,
) -> ! {
    panic!(
        "{which}: the owned per-node ForeignScan parallel method takes &mut ParallelContext, \
         but the parallel-executor walk holds the DSM-owned ParallelContextHandle and no \
         bridge from the handle to the owned ParallelContext value exists yet \
         (parallel-DSM-carrier keystone pending)"
    )
}

fn customscan_no_owned_pcxt(
    which: &str,
    _node: &mut mcx::PgBox<'_, types_nodes::nodeforeigncustom::CustomScanState<'_>>,
) -> ! {
    panic!(
        "{which}: the owned per-node CustomScan parallel method takes &mut ParallelContext, \
         but the parallel-executor walk holds the DSM-owned ParallelContextHandle and no \
         bridge from the handle to the owned ParallelContext value exists yet \
         (parallel-DSM-carrier keystone pending)"
    )
}
