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
//! `QueryDesc` lifecycle are owned (`execMain` / `nodes::QueryDesc`) and
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
use execparallel::{
    dsa_pointer_is_valid, DsaAreaHandle, DsmSegmentHandle, FixedParallelExecutorState,
    ParallelExecutorInfo, SerializeCursor, SharedExecutorInstrumentation, ShmTocHandle, Size,
    TuplesNeeded, DsaPointer, INVALID_DSA_POINTER, PGJIT_NONE,
};
use nodes::bitmapset::Bitmapset;
use nodes::querydesc::QueryDesc;
use nodes::{EStateData, PlanStateNode};
use types_core::instrument::{BufferUsage, WalUsage};
use nodes::instrument::Instrumentation;

use transam_parallel as parallel;
use execParallel_support_seams as sup;
use tqueue_seams as tqueue;
use instrument_seams as instr;
use shm_mq_seams as shmmq;
use dsa_seams as dsa;

use execMain as execMain;

use nodeAppend as nodeAppend;
use nodeBitmapHeapscan as nodeBitmapHeap;
use nodeBitmapIndexscan as nodeBitmapIndex;
use nodeHash as nodeHash;
use nodeHashjoin as nodeHashjoin;
use nodeIndexonlyscan as nodeIndexOnly;
use nodeIndexscan as nodeIndex;
use nodeIncrementalSort as nodeIncrementalSort;
use nodeAgg as nodeAgg;
use nodeMemoize as nodeMemoize;
use nodeSeqscan as nodeSeqscan;
use nodeSort as nodeSort;

/// `(AggState *) planstate` — recover the concrete `AggStateData` carried behind
/// the `PlanStateNode::Agg` variant's erased `AggStateLive` trait object
/// (`AggStateData` lives above `types-nodes`, so the enum holds it tag-checked).
/// The C parallel hooks all begin with the `(AggState *) node` cast; this is its
/// owned-model rendering. Panics on a tag mismatch — only the canonical
/// `AggStateData` ever rides as a `dyn AggStateLive`.
#[inline]
fn agg_state_mut<'a, 'mcx>(
    live: &'a mut (dyn nodes::aggstate_carrier::AggStateLive<'mcx> + 'mcx),
) -> &'a mut nodeAgg::aggstate::AggStateData<'mcx> {
    nodes::aggstate_carrier::downcast_agg_state_mut::<nodeAgg::aggstate::AggStateData<'mcx>>(
        live,
    )
    .expect("PlanStateNode::Agg carries the canonical AggStateData")
}

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
    core::mem::size_of::<execparallel::JitInstrumentation>();
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
    execParallel_seams::ExecParallelCreateReaders::set(ExecParallelCreateReaders);
    execParallel_seams::ExecParallelFinish::set(ExecParallelFinish);
    execParallel_seams::ParallelQueryMain::set(ParallelQueryMain);
    // The de-handled owned entry points consumed by nodeGather/nodeGatherMerge.
    execParallel_seams::exec_init_parallel_plan_owned::set(
        ExecInitParallelPlan,
    );
    execParallel_seams::exec_parallel_reinitialize_owned::set(
        ExecParallelReinitialize,
    );
    execParallel_seams::ExecParallelCleanup::set(ExecParallelCleanup);
}

// ===========================================================================
// 1. ExecSerializePlan (execParallel.c:145-221)
// ===========================================================================

/// `ExecSerializePlan(plan, estate)` — create a serialized representation of the
/// plan to be sent to each worker. Returns `nodeToString(pstmt)`.
///
/// `plan` is the leader plan node (C `planstate->plan`). The plan-fix-up +
/// serialization (copyObject(plan) → clear resjunk → build the dummy
/// PlannedStmt → nodeToString) is the worker plan-shipping path, owned by
/// outfuncs; reached over the support seam.
fn ExecSerializePlan(
    mcx: Mcx<'_>,
    plan: &nodes::nodes::Node<'_>,
    estate: &mut EStateData<'_>,
) -> PgResult<String> {
    sup::serialize_plan_for_workers::call(mcx, plan, estate)
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
    pcxt: execparallel::ParallelContextHandle,
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
        // case T_IncrementalSortState: ExecIncrementalSortEstimate(..);
        PlanStateNode::IncrementalSort(node) => {
            nodeIncrementalSort::ExecIncrementalSortEstimate(node, pcxt)?;
        }
        // case T_AggState: ExecAggEstimate(..);
        PlanStateNode::Agg(live) => {
            let node = agg_state_mut(live.as_mut());
            nodeAgg::ExecAggEstimate(node, pcxt)?;
        }
        // case T_MemoizeState: ExecMemoizeEstimate(..);
        PlanStateNode::Memoize(node) => {
            nodeMemoize::ExecMemoizeEstimate(node, pcxt)?;
        }
        // case T_AggState: ExecAggEstimate(..) — see ExecParallelRetrieveInstrumentation
        // for why the AggState arm lands with nodeAgg (the concrete AggStateData
        // lives above this crate behind the AggStateLive carrier).
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
    pcxt: execparallel::ParallelContextHandle,
    instrumentation: Option<execparallel::InstrumentationHandle>,
    nnodes: &mut i32,
) -> PgResult<bool> {
    // If instrumentation is enabled, initialize slot for this node.
    if let Some(sei) = instrumentation {
        parallel::set_sei_plan_node_id(sei, *nnodes, planstate.plan_node_id());
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
        PlanStateNode::IncrementalSort(node) => {
            nodeIncrementalSort::ExecIncrementalSortInitializeDSM(node, pcxt)?;
        }
        PlanStateNode::Agg(live) => {
            let node = agg_state_mut(live.as_mut());
            nodeAgg::ExecAggInitializeDSM(node, pcxt)?;
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
    pcxt: execparallel::ParallelContextHandle,
    reinitialize: bool,
) -> PgResult<PgVec<'mcx, execparallel::ShmMqAttachHandle>> {
    let nworkers = parallel::pcxt_nworkers(pcxt);

    // Skip this if no workers.
    if nworkers == 0 {
        return Ok(PgVec::new_in(mcx));
    }

    let toc = parallel::pcxt_toc(pcxt);
    let seg = parallel::pcxt_seg(pcxt);

    // If not reinitializing, allocate space from the DSM for the queues;
    // otherwise, find the already allocated space.
    let tqueuespace: SerializeCursor = if !reinitialize {
        parallel::shm_toc_allocate(toc, mul_size(PARALLEL_TUPLE_QUEUE_SIZE, nworkers as usize)?)
    } else {
        parallel::shm_toc_lookup(toc, PARALLEL_KEY_TUPLE_QUEUE, false).ok_or_else(|| {
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
        parallel::shm_toc_insert(toc, PARALLEL_KEY_TUPLE_QUEUE, tqueuespace);
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
    send_params: Option<&Bitmapset<'mcx>>,
    nworkers: i32,
    tuples_needed: TuplesNeeded,
) -> PgResult<ParallelExecutorInfo<'mcx>> {
    let empty_bms;
    let send_params: &Bitmapset<'mcx> = match send_params {
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
    sup::exec_set_param_plan_multi::call(send_params, per_tuple_econtext, estate)?;

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
    //   pstmt_data = ExecSerializePlan(planstate->plan, estate);
    // `planstate->plan` is a back-pointer into the plan tree (`&'mcx Node`), so
    // copy out the reference before re-borrowing `estate` mutably.
    let leader_plan: &nodes::nodes::Node<'mcx> = planstate
        .ps_head()
        .plan
        .ok_or_else(|| PgError::error("ExecInitParallelPlan: planstate->plan is NULL"))?;
    let pstmt_data = ExecSerializePlan(mcx, leader_plan, estate)?;

    // Create a parallel context.
    let pcxt = parallel::create_parallel_context(
        mcx,
        String::from("postgres"),
        String::from("ParallelQueryMain"),
        nworkers,
    )?;
    pei.pcxt = Some(pcxt);

    let estimator = parallel::pcxt_estimator(pcxt);
    let pcxt_nworkers = parallel::pcxt_nworkers(pcxt);

    // Estimate space for fixed-size state.
    parallel::shm_toc_estimate_chunk(estimator, SIZEOF_FIXED_STATE);
    parallel::shm_toc_estimate_keys(estimator, 1);

    // Estimate space for query text.
    let query_text = estate
        .es_sourceText
        .as_ref()
        .map(|s| s.as_str().to_string())
        .ok_or_else(|| PgError::error("ExecInitParallelPlan: estate->es_sourceText is NULL"))?;
    let query_len: i32 = query_text.len() as i32;
    parallel::shm_toc_estimate_chunk(estimator, query_len as Size + 1);
    parallel::shm_toc_estimate_keys(estimator, 1);

    // Estimate space for serialized PlannedStmt.
    let pstmt_len: i32 = pstmt_data.len() as i32 + 1;
    parallel::shm_toc_estimate_chunk(estimator, pstmt_len as Size);
    parallel::shm_toc_estimate_keys(estimator, 1);

    // Estimate space for serialized ParamListInfo.
    let param_li = estate.es_param_list_info.clone();
    let paramlistinfo_len: i32 = sup::estimate_param_list_space::call(param_li.clone()) as i32;
    parallel::shm_toc_estimate_chunk(estimator, paramlistinfo_len as Size);
    parallel::shm_toc_estimate_keys(estimator, 1);

    // Estimate space for BufferUsage.
    parallel::shm_toc_estimate_chunk(
        estimator,
        mul_size(SIZEOF_BUFFER_USAGE, pcxt_nworkers as usize)?,
    );
    parallel::shm_toc_estimate_keys(estimator, 1);

    // Same for WalUsage.
    parallel::shm_toc_estimate_chunk(estimator, mul_size(SIZEOF_WAL_USAGE, pcxt_nworkers as usize)?);
    parallel::shm_toc_estimate_keys(estimator, 1);

    // Estimate space for tuple queues.
    parallel::shm_toc_estimate_chunk(
        estimator,
        mul_size(PARALLEL_TUPLE_QUEUE_SIZE, pcxt_nworkers as usize)?,
    );
    parallel::shm_toc_estimate_keys(estimator, 1);

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
        parallel::shm_toc_estimate_chunk(estimator, instrumentation_len as Size);
        parallel::shm_toc_estimate_keys(estimator, 1);

        // Estimate space for JIT instrumentation, if required.
        if es_jit_flags != PGJIT_NONE {
            jit_instrumentation_len =
                OFFSET_OF_JIT_INSTR as i32 + SIZEOF_JIT_INSTRUMENTATION as i32 * nworkers;
            parallel::shm_toc_estimate_chunk(estimator, jit_instrumentation_len as Size);
            parallel::shm_toc_estimate_keys(estimator, 1);
        }
    }

    // Estimate space for DSA area.
    parallel::shm_toc_estimate_chunk(estimator, dsa_minsize);
    parallel::shm_toc_estimate_keys(estimator, 1);

    // Everyone's had a chance to ask for space, so now create the DSM.
    parallel::initialize_parallel_dsm(mcx, pcxt)?;

    let toc = parallel::pcxt_toc(pcxt);
    let seg = parallel::pcxt_seg(pcxt);

    // Store fixed-size state.
    let fpes_chunk = parallel::shm_toc_allocate(toc, SIZEOF_FIXED_STATE);
    let fpes = parallel::store_fixed_state(
        fpes_chunk,
        FixedParallelExecutorState {
            tuples_needed,
            param_exec: INVALID_DSA_POINTER,
            eflags: estate.es_top_eflags,
            jit_flags: es_jit_flags,
        },
    );
    parallel::shm_toc_insert(toc, PARALLEL_KEY_EXECUTOR_FIXED, fpes_chunk);

    // Store query string.
    let query_chunk = parallel::shm_toc_allocate(toc, query_len as Size + 1);
    parallel::store_cstring(query_chunk, query_text);
    parallel::shm_toc_insert(toc, PARALLEL_KEY_QUERY_TEXT, query_chunk);

    // Store serialized PlannedStmt.
    let pstmt_chunk = parallel::shm_toc_allocate(toc, pstmt_len as Size);
    parallel::store_cstring(pstmt_chunk, pstmt_data);
    parallel::shm_toc_insert(toc, PARALLEL_KEY_PLANNEDSTMT, pstmt_chunk);

    // Store serialized ParamListInfo.
    let paramlistinfo_chunk = parallel::shm_toc_allocate(toc, paramlistinfo_len as Size);
    parallel::shm_toc_insert(toc, PARALLEL_KEY_PARAMLISTINFO, paramlistinfo_chunk);
    sup::serialize_param_list::call(param_li, paramlistinfo_chunk)?;

    // Allocate space for each worker's BufferUsage; no need to initialize.
    let bufusage_chunk =
        parallel::shm_toc_allocate(toc, mul_size(SIZEOF_BUFFER_USAGE, pcxt_nworkers as usize)?);
    parallel::shm_toc_insert(toc, PARALLEL_KEY_BUFFER_USAGE, bufusage_chunk);
    pei.buffer_usage = bufusage_chunk;

    // Same for WalUsage.
    let walusage_chunk =
        parallel::shm_toc_allocate(toc, mul_size(SIZEOF_WAL_USAGE, pcxt_nworkers as usize)?);
    parallel::shm_toc_insert(toc, PARALLEL_KEY_WAL_USAGE, walusage_chunk);
    pei.wal_usage = walusage_chunk;

    // Set up the tuple queues that the workers will write into.
    pei.tqueue = ExecParallelSetupTupleQueues(mcx, pcxt, false)?;

    // We don't need the TupleQueueReaders yet, though.
    pei.reader = PgVec::new_in(mcx);

    // If instrumentation options were supplied, allocate space for the data.
    if es_instrument != 0 {
        let instr_chunk = parallel::shm_toc_allocate(toc, instrumentation_len as Size);
        let instrumentation = parallel::store_instrumentation_header(
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
        parallel::shm_toc_insert(toc, PARALLEL_KEY_INSTRUMENTATION, instr_chunk);
        pei.instrumentation = Some(instrumentation);

        if es_jit_flags != PGJIT_NONE {
            let jit_chunk = parallel::shm_toc_allocate(toc, jit_instrumentation_len as Size);
            let jit_instrumentation =
                parallel::store_jit_instrumentation_header(jit_chunk, nworkers);
            parallel::shm_toc_insert(toc, PARALLEL_KEY_JIT_INSTRUMENTATION, jit_chunk);
            pei.jit_instrumentation = Some(jit_instrumentation);
        }
    }

    // Create a DSA area usable by the leader and all workers. (If we failed to
    // create a DSM and are using private memory instead, skip this.)
    if let Some(seg) = seg {
        let area_chunk = parallel::shm_toc_allocate(toc, dsa_minsize);
        parallel::shm_toc_insert(toc, PARALLEL_KEY_DSA, area_chunk);
        let area =
            dsa::dsa_create_in_place::call(area_chunk, dsa_minsize, LWTRANCHE_PARALLEL_QUERY_DSA, seg);
        pei.area = Some(area);

        // Serialize parameters, if any, using DSA storage.
        if !sup::bms_is_empty::call(send_params) {
            pei.param_exec = SerializeParamExecParams(estate, send_params, area)?;
            parallel::set_fixed_param_exec(fpes, pei.param_exec);
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
    let nworkers = parallel::pcxt_nworkers_launched(pcxt);

    debug_assert!(pei.reader.is_empty());

    if nworkers > 0 {
        let mut reader = mcx::vec_with_capacity_in(mcx, nworkers as usize)?;
        for i in 0..nworkers {
            let tqueue_i = pei.tqueue[i as usize];
            shmmq::shm_mq_set_handle::call(tqueue_i, parallel::pcxt_worker_bgwhandle(pcxt, i));
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
    send_params: Option<&Bitmapset<'mcx>>,
) -> PgResult<()> {
    let empty_bms;
    let send_params: &Bitmapset<'mcx> = match send_params {
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
    sup::exec_set_param_plan_multi::call(send_params, per_tuple_econtext, estate)?;

    parallel::reinitialize_parallel_dsm(pcxt)?;
    pei.tqueue = ExecParallelSetupTupleQueues(mcx, pcxt, true)?;
    pei.reader = PgVec::new_in(mcx);
    pei.finished = false;

    let toc = parallel::pcxt_toc(pcxt);
    let fpes_chunk = parallel::shm_toc_lookup(toc, PARALLEL_KEY_EXECUTOR_FIXED, false)
        .ok_or_else(|| PgError::error("ExecParallelReinitialize: PARALLEL_KEY_EXECUTOR_FIXED present"))?;
    let fpes = parallel::fixed_state_from_chunk(fpes_chunk);

    // Free any serialized parameters from the last round.
    if dsa_pointer_is_valid(parallel::fixed_param_exec(fpes)) {
        let area = pei
            .area
            .ok_or_else(|| PgError::error("ExecParallelReinitialize: pei->area is live during reinit"))?;
        dsa::dsa_free::call(area, parallel::fixed_param_exec(fpes));
        parallel::set_fixed_param_exec(fpes, INVALID_DSA_POINTER);
    }

    // Serialize current parameter values if required.
    if !sup::bms_is_empty::call(send_params) {
        let area = pei
            .area
            .ok_or_else(|| PgError::error("ExecParallelReinitialize: pei->area is live during reinit"))?;
        pei.param_exec = SerializeParamExecParams(estate, send_params, area)?;
        parallel::set_fixed_param_exec(fpes, pei.param_exec);
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
    pcxt: execparallel::ParallelContextHandle,
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

/// `ExecParallelRetrieveInstrumentation` (execParallel.c:1034-1110) — copy
/// instrumentation about this node and its descendants from dynamic shared
/// memory into the leader's owned `PlanState` tree.
///
/// For each node: find its DSM slot by `plan_node_id`, accumulate every worker's
/// per-node `Instrumentation` into the leader's `planstate->instrument`
/// (`InstrAggNode`), and stash the per-worker array on the node's
/// `worker_instrument` carrier so `EXPLAIN ANALYZE` can print each
/// `Worker N: actual rows=...` line. (The per-node-type `Exec*Retrieve` hooks —
/// IndexScan/Sort/Hash/Agg/Memoize/BitmapHeap shared-state pull — are not
/// reached by the `count(*)`/SeqScan path and land with those nodes.)
fn ExecParallelRetrieveInstrumentation<'mcx>(
    mcx: Mcx<'mcx>,
    planstate: &mut PlanStateNode<'mcx>,
    sei: execparallel::InstrumentationHandle,
) -> PgResult<()> {
    let plan_node_id = planstate.plan_node_id();

    // Accumulate the statistics from all workers, and keep the per-worker detail.
    let per_worker = sup::retrieve_instr_from_dsm::call(sei, plan_node_id)?;
    let head = planstate.ps_head_mut();
    if let Some(dst) = head.instrument.as_deref_mut() {
        for w in &per_worker {
            instr_agg_node_local(dst, w);
        }
    }
    // Store the per-worker detail (C: palloc the WorkerInstrumentation in the
    // per-query context, memcpy the worker array in). The owned carrier holds
    // the same `num_workers`-long array directly.
    head.worker_instrument = if per_worker.is_empty() {
        None
    } else {
        Some(per_worker)
    };

    // Perform any node-type-specific work that needs to be done.
    //   switch (nodeTag(planstate)) { case T_SortState: ExecSortRetrieveInstrumentation(..); ... }
    // (The IndexScan/IndexOnlyScan/BitmapIndexScan/IncrementalSort/Hash/Agg/
    // Memoize/BitmapHeap shared-state pulls land with those nodes; Sort/Hash are
    // wired here.)
    // The per-query context the C `Exec*RetrieveInstrumentation` palloc's the
    // backend-local copy in (`planstate->state->es_query_cxt`), threaded in by
    // the caller (the owned model does not set the `ps.state` back-link).
    match planstate {
        PlanStateNode::Sort(node) => {
            nodeSort::ExecSortRetrieveInstrumentation(mcx, node)?;
        }
        PlanStateNode::IncrementalSort(node) => {
            nodeIncrementalSort::ExecIncrementalSortRetrieveInstrumentation(mcx, node)?;
        }
        PlanStateNode::Agg(live) => {
            let node = agg_state_mut(live.as_mut());
            nodeAgg::ExecAggRetrieveInstrumentation(mcx, node)?;
        }
        PlanStateNode::Hash(node) => {
            nodeHash::instrument::ExecHashRetrieveInstrumentation(mcx, node)?;
        }
        _ => {}
    }

    for child in planstate.planstate_tree_walker_children_mut() {
        ExecParallelRetrieveInstrumentation(mcx, child, sei)?;
    }
    Ok(())
}

/// `InstrAggNode(dst, add)` — the leader-side fold of one worker's per-node
/// `Instrumentation` into the leader's. The body lives in
/// `backend-executor-instrument`; reached here through the `instr_agg_node`
/// seam to avoid an execParallel→instrument crate dependency cycle.
fn instr_agg_node_local(dst: &mut Instrumentation, add: &Instrumentation) {
    instr::instr_agg_node::call(dst, *add);
}

/// `ExecParallelReportInstrumentation` (execParallel.c:1287-1325) — write the
/// worker's per-node `Instrumentation` into the DSM
/// `SharedExecutorInstrumentation`, so the leader can retrieve it.
///
/// For each node: `InstrEndLoop(planstate->instrument)`, then find the DSM slot
/// by `plan_node_id` and `InstrAggNode(&instrument[ParallelWorkerNumber],
/// planstate->instrument)` (done DSM-side by the `report_instr_to_dsm` seam).
fn ExecParallelReportInstrumentation<'mcx>(
    planstate: &mut PlanStateNode<'mcx>,
    sei: execparallel::InstrumentationHandle,
    worker: i32,
) -> PgResult<()> {
    let plan_node_id = planstate.plan_node_id();
    let head = planstate.ps_head_mut();
    if let Some(this_instr) = head.instrument.as_deref_mut() {
        instr::instr_end_loop::call(this_instr)?;
        // Add our statistics to the per-node, per-worker totals (DSM-side).
        sup::report_instr_to_dsm::call(sei, plan_node_id, worker, *this_instr)?;
    }

    for child in planstate.planstate_tree_walker_children_mut() {
        ExecParallelReportInstrumentation(child, sei, worker)?;
    }
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
    let nworkers = parallel::pcxt_nworkers_launched(pcxt);

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
    parallel::wait_for_parallel_workers_to_finish(pcxt)?;

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
    mcx: Mcx<'mcx>,
    pei: &mut ParallelExecutorInfo<'mcx>,
    planstate: &mut PlanStateNode<'mcx>,
) -> PgResult<()> {
    // Accumulate instrumentation, if any: walk the leader's owned PlanState tree
    // pulling each node's per-worker `Instrumentation` out of the DSM
    // `SharedExecutorInstrumentation`, folding it into the leader's
    // `planstate->instrument` and stashing the per-worker detail on the node's
    // `worker_instrument` carrier (for EXPLAIN ANALYZE per-worker lines).
    if let Some(sei) = pei.instrumentation {
        ExecParallelRetrieveInstrumentation(mcx, planstate, sei)?;
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
        parallel::destroy_parallel_context(pcxt)?;
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
) -> PgResult<execparallel::DestReceiverHandle> {
    let mqspace = parallel::shm_toc_lookup(toc, PARALLEL_KEY_TUPLE_QUEUE, false)
        .ok_or_else(|| PgError::error("ExecParallelGetReceiver: PARALLEL_KEY_TUPLE_QUEUE present"))?;
    // mqspace += ParallelWorkerNumber * PARALLEL_TUPLE_QUEUE_SIZE
    // C: mq = (shm_mq *) mqspace — the worker *casts* the leader-created queue,
    // it does not re-create it (that would wipe the leader's mq_set_receiver).
    let mq = shmmq::shm_mq_at::call(
        mqspace,
        parallel::parallel_worker_number(),
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
    receiver: execparallel::DestReceiverHandle,
    instrument_options: i32,
) -> PgResult<QueryDesc> {
    // Get the query string from shared memory.
    let query_chunk = parallel::shm_toc_lookup(toc, PARALLEL_KEY_QUERY_TEXT, false)
        .ok_or_else(|| PgError::error("ExecParallelGetQueryDesc: PARALLEL_KEY_QUERY_TEXT present"))?;
    let query_string = parallel::cursor_cstring(query_chunk)?;

    // Reconstruct leader-supplied PlannedStmt.
    let pstmt_chunk = parallel::shm_toc_lookup(toc, PARALLEL_KEY_PLANNEDSTMT, false)
        .ok_or_else(|| PgError::error("ExecParallelGetQueryDesc: PARALLEL_KEY_PLANNEDSTMT present"))?;
    let pstmt = parallel::cursor_cstring(pstmt_chunk)?;

    // Reconstruct ParamListInfo.
    let param_chunk = parallel::shm_toc_lookup(toc, PARALLEL_KEY_PARAMLISTINFO, false)
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
    pwcxt: execparallel::ParallelWorkerContextHandle,
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
        PlanStateNode::IncrementalSort(node) => {
            nodeIncrementalSort::ExecIncrementalSortInitializeWorker(node, pwcxt)?;
        }
        PlanStateNode::Agg(live) => {
            let node = agg_state_mut(live.as_mut());
            nodeAgg::ExecAggInitializeWorker(node, pwcxt)?;
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
    let fpes_chunk = parallel::shm_toc_lookup(toc, PARALLEL_KEY_EXECUTOR_FIXED, false)
        .ok_or_else(|| PgError::error("ParallelQueryMain: PARALLEL_KEY_EXECUTOR_FIXED present"))?;
    let fpes = parallel::fixed_state_from_chunk(fpes_chunk);

    // Set up DestReceiver, SharedExecutorInstrumentation, and QueryDesc.
    let receiver = ExecParallelGetReceiver(seg, toc)?;
    let instrumentation = parallel::shm_toc_lookup(toc, PARALLEL_KEY_INSTRUMENTATION, true)
        .map(parallel::instrumentation_from_chunk);
    if let Some(sei) = instrumentation {
        instrument_options = parallel::sei_instrument_options(sei);
    }
    let jit_instrumentation = parallel::shm_toc_lookup(toc, PARALLEL_KEY_JIT_INSTRUMENTATION, true)
        .map(parallel::jit_instrumentation_from_chunk);
    let mut query_desc = ExecParallelGetQueryDesc(mcx, toc, receiver, instrument_options)?;

    // Setting debug_query_string for individual workers.
    let source_text = sup::query_desc_source_text_owned::call(&query_desc)?;
    sup::set_debug_query_string::call(source_text.clone());

    // Report workers' query for monitoring purposes.
    sup::pgstat_report_activity_running::call(source_text);

    // Attach to the dynamic shared memory area.
    let area_chunk = parallel::shm_toc_lookup(toc, PARALLEL_KEY_DSA, false)
        .ok_or_else(|| PgError::error("ParallelQueryMain: PARALLEL_KEY_DSA present"))?;
    let area = dsa::dsa_attach_in_place::call(area_chunk, seg);

    // Start up the executor.
    sup::set_query_desc_jit_flags_owned::call(&mut query_desc, parallel::fixed_jit_flags(fpes));
    execMain::ExecutorStart(&mut query_desc, parallel::fixed_eflags(fpes))?;

    // Special executor initialization steps for parallel workers:
    //   estate->es_query_dsa = area;
    //   if (DsaPointerIsValid(fpes->param_exec))
    //       RestoreParamExecParams(start_address, queryDesc->estate);
    //   ExecParallelInitializeWorker(queryDesc->planstate, &pwcxt);
    let pwcxt = parallel::make_parallel_worker_context(seg, toc);
    let fixed_param_exec = parallel::fixed_param_exec(fpes);
    let paramexec_cursor = if dsa_pointer_is_valid(fixed_param_exec) {
        Some(dsa::dsa_get_address::call(area, fixed_param_exec))
    } else {
        None
    };
    let tuples_needed = parallel::fixed_tuples_needed(fpes);

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
        execProcnode::execProcnode_run_end::exec_set_tuple_bound(
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
    let buffer_usage = parallel::shm_toc_lookup(toc, PARALLEL_KEY_BUFFER_USAGE, false)
        .ok_or_else(|| PgError::error("ParallelQueryMain: PARALLEL_KEY_BUFFER_USAGE present"))?;
    let wal_usage = parallel::shm_toc_lookup(toc, PARALLEL_KEY_WAL_USAGE, false)
        .ok_or_else(|| PgError::error("ParallelQueryMain: PARALLEL_KEY_WAL_USAGE present"))?;
    let parallel_worker_number = parallel::parallel_worker_number();
    sup::instr_end_parallel_query::call(buffer_usage, wal_usage, parallel_worker_number);

    // Report instrumentation data if any instrumentation options are set: walk
    // the worker's owned PlanState tree, `InstrEndLoop` each node, and aggregate
    // its `Instrumentation` into this worker's per-node DSM slot so the leader
    // can retrieve it in ExecParallelCleanup.
    if let Some(sei) = instrumentation {
        query_desc.with_plan_and_estate_mut(|_plan, _pstmt, _estate, planstate_slot| -> PgResult<()> {
            let planstate = planstate_slot.as_deref_mut().ok_or_else(|| {
                PgError::error("ParallelQueryMain: queryDesc->planstate is NULL at report")
            })?;
            ExecParallelReportInstrumentation(planstate, sei, parallel_worker_number)
        })?;
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
    _node: &mut mcx::PgBox<'_, nodes::nodeforeigncustom::ForeignScanState<'_>>,
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
    _node: &mut mcx::PgBox<'_, nodes::nodeforeigncustom::CustomScanState<'_>>,
) -> ! {
    panic!(
        "{which}: the owned per-node CustomScan parallel method takes &mut ParallelContext, \
         but the parallel-executor walk holds the DSM-owned ParallelContextHandle and no \
         bridge from the handle to the owned ParallelContext value exists yet \
         (parallel-DSM-carrier keystone pending)"
    )
}
