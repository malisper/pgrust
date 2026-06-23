//! Hash-build instrumentation and the parallel-DSM node hooks.
//!
//! The four parallel-executor hooks (`ExecHashEstimate`, `ExecHashInitializeDSM`,
//! `ExecHashInitializeWorker`, `ExecHashRetrieveInstrumentation`) now take the
//! OWNED `&mut HashState<'mcx>` — exactly as the C functions take `HashState *`
//! — mirroring the already-owned nodes (`nodeBitmapHeapscan`/`nodeHashjoin`/
//! `nodeAgg`). They read `node->ps.instrument` and `node->ps.plan->plan_node_id`
//! directly from the owned `HashState`/`PlanStateData` fields (the old
//! `parallel_sup::hash_*` handle seams are RETIRED).
//!
//! The DSM-resident `SharedHashInfo` (`{ int num_workers; HashInstrumentation
//! hinstrument[]; }`) is reached through the ORTHOGONAL `shm_toc` support seams
//! (`pcxt_estimator`/`pcxt_toc`/`pwcxt_toc`/`shm_toc_estimate_chunk`/
//! `shm_toc_estimate_keys`/`shm_toc_allocate`/`shm_toc_insert`/`shm_toc_lookup`),
//! which take owned handles and keep the DSM layout behind the parallel owner.
//! The chunk bytes are placed/attached through the typed shared-DSM-object flex
//! primitive (`types_parallel::shared_dsm_object::place_flex`/`attach_flex`), so
//! `SharedHashInfo` lives DIRECTLY in the segment that every worker maps.

use mcx::{Mcx, PgVec};
use types_error::PgResult;
use ::nodes::nodehash::{
    HashInstrumentSlot, HashInstrumentation, HashJoinTableData, HashState, SharedHashInfo,
    SharedHashInfoHeader,
};

use transam_parallel as parallel_sup;
use types_parallel::shared_dsm_object;

/// `node->ps.plan->plan_node_id` — the DSM toc key. Mirrors the C
/// `node->ps.plan->plan_node_id` dereference.
#[inline]
fn hash_plan_node_id(node: &HashState) -> i32 {
    node.ps
        .plan
        .map(|n| n.plan_head().plan_node_id)
        .expect("HashState.ps.plan")
}

/// `offsetof(SharedHashInfo, hinstrument) + nworkers * sizeof(HashInstrumentation)`
/// — the byte size of a `SharedHashInfo` carrying `nworkers` per-worker slots.
/// (`offsetof(SharedHashInfo, hinstrument)` is `sizeof(SharedHashInfoHeader)`
/// MAXALIGN'd up to `HashInstrumentation`'s alignment.)
#[inline]
fn shared_hash_info_size(nworkers: usize) -> usize {
    use core::mem::{align_of, size_of};
    let h = size_of::<SharedHashInfoHeader>();
    let a = align_of::<HashInstrumentation>();
    let off = (h + a - 1) & !(a - 1);
    off + nworkers * size_of::<HashInstrumentation>()
}

/// `ExecHashEstimate(HashState *node, ParallelContext *pcxt)`
/// (nodeHash.c:2761) — reserve DSM space for the shared instrumentation area.
pub fn ExecHashEstimate(
    node: &mut HashState<'_>,
    pcxt: execparallel::ParallelContextHandle,
) -> PgResult<()> {
    // don't need this if not instrumenting or no workers
    //   if (!node->ps.instrument || pcxt->nworkers == 0) return;
    if node.ps.instrument.is_none() || parallel_sup::pcxt_nworkers(pcxt) == 0 {
        return Ok(());
    }

    let nworkers = parallel_sup::pcxt_nworkers(pcxt) as usize;

    //   size = mul_size(pcxt->nworkers, sizeof(HashInstrumentation));
    //   size = add_size(size, offsetof(SharedHashInfo, hinstrument));
    let size = shared_dsm_object::estimate_flex(shared_hash_info_size(nworkers));

    //   shm_toc_estimate_chunk(&pcxt->estimator, size);
    //   shm_toc_estimate_keys(&pcxt->estimator, 1);
    let estimator = parallel_sup::pcxt_estimator(pcxt);
    parallel_sup::shm_toc_estimate_chunk(estimator, size);
    parallel_sup::shm_toc_estimate_keys(estimator, 1);
    Ok(())
}

/// `ExecHashInitializeDSM(HashState *node, ParallelContext *pcxt)`
/// (nodeHash.c:2780) — set up the shared `SharedHashInfo` instrumentation area.
pub fn ExecHashInitializeDSM(
    node: &mut HashState<'_>,
    pcxt: execparallel::ParallelContextHandle,
) -> PgResult<()> {
    // don't need this if not instrumenting or no workers
    //   if (!node->ps.instrument || pcxt->nworkers == 0) return;
    let nworkers = parallel_sup::pcxt_nworkers(pcxt);
    if node.ps.instrument.is_none() || nworkers == 0 {
        return Ok(());
    }

    let plan_node_id = hash_plan_node_id(node);

    //   size = offsetof(SharedHashInfo, hinstrument) +
    //          pcxt->nworkers * sizeof(HashInstrumentation);
    let size = shared_hash_info_size(nworkers as usize);

    //   node->shared_info = (SharedHashInfo *) shm_toc_allocate(pcxt->toc, size);
    let toc = parallel_sup::pcxt_toc(pcxt);
    let chunk = parallel_sup::shm_toc_allocate(toc, shared_dsm_object::estimate_flex(size));

    // `ExecHashInitializeDSM` does NOT gate on `pcxt->seg` (unlike the hash-join
    // hook); `place_flex`'s `_seg` argument is unused (the placement is the raw
    // chunk address). A parallel query with instrument-bearing workers always
    // has a real DSM segment.
    let seg = parallel_sup::pcxt_seg(pcxt)
        .expect("ExecHashInitializeDSM: instrumenting parallel query without a DSM segment");

    //   /* Each per-worker area must start out as zeroes. */
    //   memset(node->shared_info, 0, size);
    //   node->shared_info->num_workers = pcxt->nworkers;
    let (_hdr, _tail) = shared_dsm_object::place_flex::<SharedHashInfoHeader, HashInstrumentation>(
        seg,
        chunk,
        nworkers as usize,
        SharedHashInfoHeader { num_workers: nworkers },
        |_i| HashInstrumentation::default(),
    );

    //   shm_toc_insert(pcxt->toc, node->ps.plan->plan_node_id, node->shared_info);
    parallel_sup::shm_toc_insert(toc, plan_node_id as u64, chunk);

    node.shared_info = Some(SharedHashInfo::Dsm {
        chunk,
        seg,
        num_workers: nworkers,
    });
    Ok(())
}

/// `ExecHashInitializeWorker(HashState *node, ParallelWorkerContext *pwcxt)`
/// (nodeHash.c:2805) — attach a worker to the shared instrumentation area.
pub fn ExecHashInitializeWorker(
    node: &mut HashState<'_>,
    pwcxt: execparallel::ParallelWorkerContextHandle,
) -> PgResult<()> {
    // don't need this if not instrumenting
    //   if (!node->ps.instrument) return;
    if node.ps.instrument.is_none() {
        return Ok(());
    }

    //   shared_info = (SharedHashInfo *)
    //       shm_toc_lookup(pwcxt->toc, node->ps.plan->plan_node_id, false);
    let plan_node_id = hash_plan_node_id(node);
    let toc = parallel_sup::pwcxt_toc(pwcxt);
    let chunk = parallel_sup::shm_toc_lookup(toc, plan_node_id as u64, false)
        .expect("ExecHashInitializeWorker: shm_toc_lookup(plan_node_id) missing");

    //   node->hinstrument = &shared_info->hinstrument[ParallelWorkerNumber];
    let seg = parallel_sup::pwcxt_seg(pwcxt);
    let worker_index = parallel_sup::parallel_worker_number();
    node.hinstrument = Some(HashInstrumentSlot::Dsm {
        chunk,
        seg,
        worker_index,
    });
    Ok(())
}

/// `*node->hinstrument` (`HashInstrumentation *`) — run `f` against this
/// process's stats slot, whether it is a backend-local `palloc0`'d struct or an
/// alias into the DSM `SharedHashInfo` array (`&shared_info->hinstrument[N]`).
/// The DSM arm reaches the element through `shared_dsm_object::with_mut` over
/// the element's in-segment address (`chunk + offsetof(hinstrument) + N *
/// sizeof(HashInstrumentation)`); the worker is the sole writer of its own slot,
/// satisfying `with_mut`'s sole-accessor obligation.
pub fn with_hinstrument_mut<R>(
    slot: &mut HashInstrumentSlot<'_>,
    f: impl FnOnce(&mut HashInstrumentation) -> R,
) -> R {
    match slot {
        HashInstrumentSlot::Local(b) => f(&mut *b),
        HashInstrumentSlot::Dsm {
            chunk,
            seg,
            worker_index,
        } => {
            use core::mem::{align_of, size_of};
            let h = size_of::<SharedHashInfoHeader>();
            let a = align_of::<HashInstrumentation>();
            let off = (h + a - 1) & !(a - 1);
            let elem = execparallel::SerializeCursor(
                chunk.0 + off + (*worker_index as usize) * size_of::<HashInstrumentation>(),
            );
            shared_dsm_object::with_mut::<HashInstrumentation, R>(*seg, elem, f)
        }
    }
}

/// `ExecHashRetrieveInstrumentation(HashState *node)` (nodeHash.c:2846) — the
/// leader copies the shared-memory stats into local storage before DSM
/// shutdown.
///
/// Takes an explicit `mcx` (the C uses `palloc`, i.e. `CurrentMemoryContext`)
/// to allocate the backend-local copy, mirroring `ExecShutdownHash`'s `mcx`
/// parameter.
pub fn ExecHashRetrieveInstrumentation<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut HashState<'mcx>,
) -> PgResult<()> {
    //   SharedHashInfo *shared_info = node->shared_info;
    //   if (shared_info == NULL) return;
    let (chunk, seg, num_workers) = match node.shared_info {
        Some(SharedHashInfo::Dsm {
            chunk,
            seg,
            num_workers,
        }) => (chunk, seg, num_workers),
        // Already a backend-local copy, or NULL: nothing to retrieve.
        _ => return Ok(()),
    };

    //   /* Replace node->shared_info with a copy in backend-local memory. */
    //   size = offsetof(SharedHashInfo, hinstrument) +
    //          shared_info->num_workers * sizeof(HashInstrumentation);
    //   node->shared_info = palloc(size);
    //   memcpy(node->shared_info, shared_info, size);
    //
    // The DSM segment is still mapped here (the C runs this before detach). Read
    // the flex array out of the segment and snapshot it into a backend-local
    // `PgVec`; `node->shared_info` then becomes the `Local` arm.
    let (_hdr, tail) =
        shared_dsm_object::attach_flex::<SharedHashInfoHeader, HashInstrumentation>(
            seg,
            chunk,
            num_workers as usize,
        );

    let mut copy: PgVec<'mcx, HashInstrumentation> =
        PgVec::with_capacity_in(num_workers as usize, mcx);
    for &elem in tail.get().iter() {
        copy.push(elem);
    }
    node.shared_info = Some(SharedHashInfo::Local {
        num_workers,
        hinstrument: copy,
    });
    Ok(())
}

// ---------------------------------------------------------------------------
// Seam shims installed into `backend-executor-nodeHash-pq-seams`.
//
// `execParallel` dispatches the per-node parallel hooks generically, holding a
// `PlanState *` (here the opaque `PlanStateHandle`); the C `ExecHashEstimate`
// etc. begin with the `(HashState *) node` cast. Recovering the live `HashState`
// from the handle is the executor's `PlanState`-pointer registry — that pointer
// table is the unported executor surface, so each shim performs the C cast
// through `resolve_hash_state` (which panics until that registry lands) and then
// runs the real, ported, OWNED-typed entry point above. Mirrors
// nodeHashjoin::resolve_hash_join_state / nodeAgg::aggapi.
// ---------------------------------------------------------------------------

/// `(HashState *) node` — recover the live `HashState` a `PlanStateHandle`
/// refers to. The executor's `PlanState` pointer registry that backs this lookup
/// is not yet ported (the execParallel PlanStateHandle re-model keystone).
fn resolve_hash_state<'mcx>(
    _node: execparallel::PlanStateHandle,
) -> &'mcx mut HashState<'mcx> {
    panic!(
        "backend-executor-nodeHash: resolving a PlanStateHandle to the live HashState needs the \
         executor PlanState pointer registry (unported); the (HashState *) node cast in the \
         ExecHash* parallel hooks cannot run yet"
    );
}

/// `CurrentMemoryContext` at the `ExecHashRetrieveInstrumentation` call site —
/// recovered from the same unported executor surface that backs
/// `resolve_hash_state`, so it shares that panic.
fn resolve_retrieve_mcx<'mcx>(_node: execparallel::PlanStateHandle) -> Mcx<'mcx> {
    panic!(
        "backend-executor-nodeHash: the CurrentMemoryContext for \
         ExecHashRetrieveInstrumentation's palloc'd copy is recovered from the unported executor \
         surface (PlanState pointer registry); cannot run yet"
    );
}

/// Seam shim for `ExecHashEstimate`.
fn exec_hash_estimate_shim(
    node: execparallel::PlanStateHandle,
    pcxt: execparallel::ParallelContextHandle,
) -> PgResult<()> {
    ExecHashEstimate(resolve_hash_state(node), pcxt)
}

/// Seam shim for `ExecHashInitializeDSM`.
fn exec_hash_initialize_dsm_shim(
    node: execparallel::PlanStateHandle,
    pcxt: execparallel::ParallelContextHandle,
) -> PgResult<()> {
    ExecHashInitializeDSM(resolve_hash_state(node), pcxt)
}

/// Seam shim for `ExecHashInitializeWorker`.
fn exec_hash_initialize_worker_shim(
    node: execparallel::PlanStateHandle,
    pwcxt: execparallel::ParallelWorkerContextHandle,
) -> PgResult<()> {
    ExecHashInitializeWorker(resolve_hash_state(node), pwcxt)
}

/// Seam shim for `ExecHashRetrieveInstrumentation`.
fn exec_hash_retrieve_instrumentation_shim(
    node: execparallel::PlanStateHandle,
) -> PgResult<()> {
    ExecHashRetrieveInstrumentation(resolve_retrieve_mcx(node), resolve_hash_state(node))
}

/// Install the four parallel-context node hooks into
/// `backend-executor-nodeHash-pq-seams`.
pub fn init_pq_seams() {
    nodeHash_pq_seams::exec_hash_estimate::set(exec_hash_estimate_shim);
    nodeHash_pq_seams::exec_hash_initialize_dsm::set(
        exec_hash_initialize_dsm_shim,
    );
    nodeHash_pq_seams::exec_hash_initialize_worker::set(
        exec_hash_initialize_worker_shim,
    );
    nodeHash_pq_seams::exec_hash_retrieve_instrumentation::set(
        exec_hash_retrieve_instrumentation_shim,
    );
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
