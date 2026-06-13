//! Hash-build instrumentation and the parallel-DSM node hooks. The four
//! parallel-executor hooks are the implementations the parallel executor
//! reaches through `backend-executor-nodeHash-pq-seams` (installed by
//! [`crate::init_seams`]).
//!
//! Like every parallel-executor hook the `execParallel.c` driver invokes, these
//! receive the node as an opaque `PlanStateHandle` (the driver only holds a
//! `PlanState *`). The `HashState` fields and the DSM-resident `SharedHashInfo`
//! are reached through `backend-executor-execParallel-support-seams`, whose
//! owner performs the actual DSM `shm_toc_allocate`/`memset`/`shm_toc_insert`/
//! `shm_toc_lookup` against the live node; those seams panic (uninstalled)
//! until that owner lands.

use core::mem::size_of;

use types_error::PgResult;
use types_execparallel::{ParallelContextHandle, ParallelWorkerContextHandle, PlanStateHandle};
use types_nodes::nodehash::{HashInstrumentation, HashJoinTableData};

use backend_executor_execParallel_support_seams as parallel_sup;

/// `offsetof(SharedHashInfo, hinstrument)` (`nodes/execnodes.h`) —
/// `SharedHashInfo` is `{ int num_workers; HashInstrumentation hinstrument[]; }`.
/// `HashInstrumentation`'s first field is `Size space_peak`-bearing, so the
/// flexible array is `MAXALIGN`'d (8) past the `int`: offset 8 on 64-bit.
const SIZEOF_SHARED_HASH_INFO_HEADER: usize = crate::MAXALIGN(size_of::<i32>());

/// `sizeof(HashInstrumentation)` — four `int`s and a `Size` (`nbuckets`,
/// `nbuckets_original`, `nbatch`, `nbatch_original`, `space_peak`), MAXALIGN'd:
/// 4*4 = 16 bytes of ints + 8-byte `Size` = 24 bytes on 64-bit.
const SIZEOF_HASH_INSTRUMENTATION: usize = crate::MAXALIGN(4 * size_of::<i32>() + size_of::<usize>());

/// `ExecHashEstimate(HashState *node, ParallelContext *pcxt)`
/// (nodeHash.c:2761) — reserve DSM space for the shared instrumentation area.
pub fn ExecHashEstimate(node: PlanStateHandle, pcxt: ParallelContextHandle) -> PgResult<()> {
    // don't need this if not instrumenting or no workers
    //   if (!node->ps.instrument || pcxt->nworkers == 0) return;
    if !parallel_sup::hash_instrument_present::call(node)
        || parallel_sup::pcxt_nworkers::call(pcxt) == 0
    {
        return Ok(());
    }

    let nworkers = parallel_sup::pcxt_nworkers::call(pcxt);

    //   size = mul_size(pcxt->nworkers, sizeof(HashInstrumentation));
    //   size = add_size(size, offsetof(SharedHashInfo, hinstrument));
    let size = (nworkers as usize) * SIZEOF_HASH_INSTRUMENTATION + SIZEOF_SHARED_HASH_INFO_HEADER;

    //   shm_toc_estimate_chunk(&pcxt->estimator, size);
    //   shm_toc_estimate_keys(&pcxt->estimator, 1);
    parallel_sup::pcxt_estimate_chunk::call(pcxt, size)?;
    parallel_sup::pcxt_estimate_keys::call(pcxt, 1)?;
    Ok(())
}

/// `ExecHashInitializeDSM(HashState *node, ParallelContext *pcxt)`
/// (nodeHash.c:2780) — set up the shared `SharedHashInfo` instrumentation area.
pub fn ExecHashInitializeDSM(
    node: PlanStateHandle,
    pcxt: ParallelContextHandle,
) -> PgResult<()> {
    // don't need this if not instrumenting or no workers
    //   if (!node->ps.instrument || pcxt->nworkers == 0) return;
    if !parallel_sup::hash_instrument_present::call(node)
        || parallel_sup::pcxt_nworkers::call(pcxt) == 0
    {
        return Ok(());
    }

    let nworkers = parallel_sup::pcxt_nworkers::call(pcxt);

    //   size = offsetof(SharedHashInfo, hinstrument) +
    //          pcxt->nworkers * sizeof(HashInstrumentation);
    let size =
        SIZEOF_SHARED_HASH_INFO_HEADER + (nworkers as usize) * SIZEOF_HASH_INSTRUMENTATION;
    let plan_node_id = parallel_sup::hash_plan_node_id::call(node);

    //   node->shared_info = (SharedHashInfo *) shm_toc_allocate(pcxt->toc, size);
    //   /* Each per-worker area must start out as zeroes. */
    //   memset(node->shared_info, 0, size);
    //   node->shared_info->num_workers = pcxt->nworkers;
    //   shm_toc_insert(pcxt->toc, node->ps.plan->plan_node_id, node->shared_info);
    parallel_sup::hash_initialize_dsm_shared_info::call(node, pcxt, nworkers, plan_node_id, size)
}

/// `ExecHashInitializeWorker(HashState *node, ParallelWorkerContext *pwcxt)`
/// (nodeHash.c:2805) — attach a worker to the shared instrumentation area.
pub fn ExecHashInitializeWorker(
    node: PlanStateHandle,
    pwcxt: ParallelWorkerContextHandle,
) -> PgResult<()> {
    // don't need this if not instrumenting
    //   if (!node->ps.instrument) return;
    if !parallel_sup::hash_instrument_present::call(node) {
        return Ok(());
    }

    //   shared_info = (SharedHashInfo *)
    //       shm_toc_lookup(pwcxt->toc, node->ps.plan->plan_node_id, false);
    //   node->hinstrument = &shared_info->hinstrument[ParallelWorkerNumber];
    let plan_node_id = parallel_sup::hash_plan_node_id::call(node);
    parallel_sup::hash_initialize_worker_shared_info::call(node, pwcxt, plan_node_id)
}

/// `ExecHashRetrieveInstrumentation(HashState *node)` (nodeHash.c:2846) — the
/// leader copies the shared-memory stats into local storage before DSM
/// shutdown.
pub fn ExecHashRetrieveInstrumentation(node: PlanStateHandle) -> PgResult<()> {
    //   SharedHashInfo *shared_info = node->shared_info;
    //   if (shared_info == NULL) return;
    if !parallel_sup::hash_shared_info_present::call(node) {
        return Ok(());
    }

    //   /* Replace node->shared_info with a copy in backend-local memory. */
    //   size = offsetof(SharedHashInfo, hinstrument) +
    //          shared_info->num_workers * sizeof(HashInstrumentation);
    //   node->shared_info = palloc(size);
    //   memcpy(node->shared_info, shared_info, size);
    let num_workers = parallel_sup::hash_shared_info_num_workers::call(node);
    let size =
        SIZEOF_SHARED_HASH_INFO_HEADER + (num_workers as usize) * SIZEOF_HASH_INSTRUMENTATION;
    parallel_sup::hash_retrieve_shared_info::call(node, size)
}

/// `ExecHashAccumInstrumentation(HashInstrumentation *instrument,
/// HashJoinTable hashtable)` (nodeHash.c:2877) — fold the live hashtable's
/// dimensions into the running instrumentation maxima. Pure field updates.
pub fn ExecHashAccumInstrumentation<'mcx>(
    instrument: &mut HashInstrumentation,
    hashtable: &HashJoinTableData<'mcx>,
) {
    instrument.nbuckets = instrument.nbuckets.max(hashtable.nbuckets);
    instrument.nbuckets_original = instrument.nbuckets_original.max(hashtable.nbuckets_original);
    instrument.nbatch = instrument.nbatch.max(hashtable.nbatch);
    instrument.nbatch_original = instrument.nbatch_original.max(hashtable.nbatch_original);
    instrument.space_peak = instrument.space_peak.max(hashtable.spacePeak);
}
