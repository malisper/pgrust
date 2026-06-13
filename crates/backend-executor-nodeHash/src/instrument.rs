//! Hash-build instrumentation and the parallel-DSM node hooks. These are the
//! implementations the parallel executor reaches through
//! `backend-executor-nodeHash-pq-seams` (installed by [`crate::init_seams`]).

use core::mem::size_of;

use mcx::Mcx;
use types_error::PgResult;
use types_execparallel::{ParallelContextHandle, ParallelWorkerContextHandle};
use types_nodes::nodehash::{
    HashInstrumentation, HashJoinTableData, HashState, SharedHashInfo,
};

use backend_access_transam_parallel_seams as parallel;
use backend_storage_ipc_shmem_seams as shmem;

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
pub fn ExecHashEstimate<'mcx>(
    node: &mut HashState<'mcx>,
    pcxt: ParallelContextHandle,
) -> PgResult<()> {
    // don't need this if not instrumenting or no workers
    if node.ps.instrument.is_none() || parallel::pcxt_nworkers::call(pcxt) == 0 {
        return Ok(());
    }

    let nworkers = parallel::pcxt_nworkers::call(pcxt);

    // size = mul_size(pcxt->nworkers, sizeof(HashInstrumentation));
    let mut size = shmem::mul_size::call(nworkers as usize, SIZEOF_HASH_INSTRUMENTATION)?;
    // size = add_size(size, offsetof(SharedHashInfo, hinstrument));
    size = shmem::add_size::call(size, SIZEOF_SHARED_HASH_INFO_HEADER)?;

    let estimator = parallel::pcxt_estimator::call(pcxt);
    parallel::shm_toc_estimate_chunk::call(estimator, size);
    parallel::shm_toc_estimate_keys::call(estimator, 1);
    Ok(())
}

/// `ExecHashInitializeDSM(HashState *node, ParallelContext *pcxt)`
/// (nodeHash.c:2780) — set up the shared `SharedHashInfo` instrumentation area.
pub fn ExecHashInitializeDSM<'mcx>(
    node: &mut HashState<'mcx>,
    pcxt: ParallelContextHandle,
) -> PgResult<()> {
    // don't need this if not instrumenting or no workers
    if node.ps.instrument.is_none() || parallel::pcxt_nworkers::call(pcxt) == 0 {
        return Ok(());
    }

    let nworkers = parallel::pcxt_nworkers::call(pcxt);

    // size = offsetof(SharedHashInfo, hinstrument) +
    //        pcxt->nworkers * sizeof(HashInstrumentation);
    let size =
        SIZEOF_SHARED_HASH_INFO_HEADER + (nworkers as usize) * SIZEOF_HASH_INSTRUMENTATION;

    // node->shared_info = (SharedHashInfo *) shm_toc_allocate(pcxt->toc, size);
    // Each per-worker area must start out as zeroes (memset 0).
    // node->shared_info->num_workers = pcxt->nworkers;
    // shm_toc_insert(pcxt->toc, node->ps.plan->plan_node_id, node->shared_info);
    //
    // The DSM-resident `SharedHashInfo` placement (a typed store into the chunk
    // returned by `shm_toc_allocate`, zeroed, then keyed into the toc by
    // `plan_node_id`) is owned by the parallel/plannode subsystem. The chunk is
    // reached through `backend-access-transam-parallel-seams`, whose
    // `shm_toc_allocate` panics (uninstalled) until that owner lands; the
    // zeroing, `num_workers` store, and toc insert are part of that same
    // not-yet-ported surface.
    let _ = nworkers;
    let toc = parallel::pcxt_toc::call(pcxt);
    let _chunk = parallel::shm_toc_allocate::call(toc, size);
    let _ = node;
    Ok(())
}

/// `ExecHashInitializeWorker(HashState *node, ParallelWorkerContext *pwcxt)`
/// (nodeHash.c:2805) — attach a worker to the shared instrumentation area.
pub fn ExecHashInitializeWorker<'mcx>(
    node: &mut HashState<'mcx>,
    pwcxt: ParallelWorkerContextHandle,
) -> PgResult<()> {
    // don't need this if not instrumenting
    if node.ps.instrument.is_none() {
        return Ok(());
    }

    // shared_info = (SharedHashInfo *)
    //     shm_toc_lookup(pwcxt->toc, node->ps.plan->plan_node_id, false);
    // node->hinstrument = &shared_info->hinstrument[ParallelWorkerNumber];
    //
    // Locating our slot reads `ParallelWorkerNumber`, then looks the shared
    // area up in the worker's DSM toc by `plan_node_id`. `ParallelWorkerNumber`
    // and the worker context's `toc`/`shm_toc_lookup` are owned by the parallel
    // subsystem; reached through its seam crate, the first such call panics
    // (uninstalled) until that owner lands. The DSM-resident `SharedHashInfo`
    // pointer wiring into `node.hinstrument` is part of that same not-yet-ported
    // surface.
    let _worker = parallel::parallel_worker_number::call();
    let _ = (node, pwcxt);
    Ok(())
}

/// `ExecHashRetrieveInstrumentation(HashState *node)` (nodeHash.c:2846) — the
/// leader copies the shared-memory stats into local storage before DSM
/// shutdown. Allocates the local copy in `mcx`.
pub fn ExecHashRetrieveInstrumentation<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut HashState<'mcx>,
) -> PgResult<()> {
    // SharedHashInfo *shared_info = node->shared_info;
    // if (shared_info == NULL) return;
    let shared_info = match node.shared_info.as_ref() {
        Some(si) => si,
        None => return Ok(()),
    };

    // Replace node->shared_info with a copy in backend-local memory.
    //
    //   size = offsetof(SharedHashInfo, hinstrument) +
    //          shared_info->num_workers * sizeof(HashInstrumentation);
    //   node->shared_info = palloc(size);
    //   memcpy(node->shared_info, shared_info, size);
    //
    // In the owned model the variable-length `hinstrument[]` is a `PgVec`; the
    // faithful copy clones the header and exactly `num_workers` per-worker
    // entries into a fresh allocation in `mcx`.
    let num_workers = shared_info.num_workers;
    let mut hinstrument = mcx::vec_with_capacity_in(mcx, num_workers as usize)?;
    for i in 0..(num_workers as usize) {
        hinstrument.push(shared_info.hinstrument[i]);
    }

    let copy = SharedHashInfo {
        num_workers,
        hinstrument,
    };
    node.shared_info = Some(mcx::alloc_in(mcx, copy)?);
    Ok(())
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
