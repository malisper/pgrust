//! Aggregate-support-function API family: the functions an aggregate's
//! transition/final function may call to introspect its calling context
//! (`AggCheckCallContext` and friends), plus the parallel-instrumentation
//! entry points that move per-worker hash-agg metrics through DSM.
//!
//! The `fcinfo->context` of a support function points at the live `AggState`
//! (or a `WindowAggState`); these resolve it. The parallel entry points are
//! the methods this unit installs into `backend-executor-nodeAgg-pq-seams`.

use mcx::MemoryContext;
use types_error::PgResult;
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_nodes::nodeagg::Aggref;
use crate::aggstate::{AggStateData, AggregateInstrumentation, SharedAggInfo};
use types_execparallel::{
    ParallelContextHandle, ParallelWorkerContextHandle, PlanStateHandle,
};

use backend_access_transam_parallel_seams as parallel_seams;
use backend_access_transam_parallel::shared_dsm_object;

/// `AGG_CONTEXT_AGGREGATE` (executor/executor.h) — called as a plain aggregate.
pub const AGG_CONTEXT_AGGREGATE: i32 = 1;
/// `AGG_CONTEXT_WINDOW` (executor/executor.h) — called as a window aggregate.
pub const AGG_CONTEXT_WINDOW: i32 = 2;

/// The C `context` of an fmgr call frame is `fmNodePtr context` — a `Node *`
/// the executor sets to the live `AggState`/`WindowAggState`. The shared
/// `FunctionCallInfoBaseData` vocabulary on main is trimmed and does not carry
/// that back-reference, so a support function reached through it observes the
/// C `fcinfo->context == NULL` case: it is not being called as an aggregate.
///
/// This helper localizes that single fact so each entry point below reads as a
/// direct transcription of the C `if (fcinfo->context && IsA(...))` guard.
#[inline]
fn agg_context<'a, 'mcx>(
    _fcinfo: &'a FunctionCallInfoBaseData<'mcx>,
) -> Option<&'a AggStateData<'mcx>> {
    None
}

/// `AggCheckCallContext(fcinfo, &aggcontext)` — report whether the function is
/// being called as an aggregate transition/final function. Returns
/// `AGG_CONTEXT_AGGREGATE` (1) / `AGG_CONTEXT_WINDOW` (2) / 0, and (when not
/// null) the appropriate aggregate memory context.
pub fn AggCheckCallContext<'mcx>(
    fcinfo: &FunctionCallInfoBaseData<'mcx>,
) -> (i32, Option<MemoryContext>) {
    if let Some(aggstate) = agg_context(fcinfo) {
        // *aggcontext = aggstate->curaggcontext->ecxt_per_tuple_memory.
        // C hands back a *pointer* to that context; the owned `MemoryContext`
        // is a non-Copy domain handle the AggState owns, so it cannot be moved
        // out of the borrow. Resolving the handle to return belongs to the
        // not-yet-landed owned-fcinfo back-reference model.
        let _ = aggstate.curaggcontext;
        panic!(
            "backend_executor_nodeAgg::AggCheckCallContext: live-AggState \
             curaggcontext handoff — unported call-frame back-reference"
        );
    }
    // The WindowAggState arm of the C is not reachable through the trimmed
    // call frame on main; with no context carried the function falls through
    // to the C `*aggcontext = NULL; return 0;` tail.
    (0, None)
}

/// `AggGetAggref(fcinfo)` — return the `Aggref` being evaluated, or `None` if
/// the function is not being called as an aggregate.
pub fn AggGetAggref<'a, 'mcx>(
    fcinfo: &'a FunctionCallInfoBaseData<'mcx>,
) -> Option<&'a Aggref<'mcx>> {
    if let Some(aggstate) = agg_context(fcinfo) {
        // check curperagg (valid when in a final function)
        if aggstate.curperagg >= 0 {
            if let Some(peragg) = aggstate
                .peragg
                .as_ref()
                .and_then(|p| p.get(aggstate.curperagg as usize))
            {
                if let Some(aggref) = peragg.aggref.as_ref() {
                    return Some(aggref);
                }
            }
        }
        // check curpertrans (valid when in a transition function)
        if aggstate.curpertrans >= 0 {
            if let Some(pertrans) = aggstate
                .pertrans
                .as_ref()
                .and_then(|p| p.get(aggstate.curpertrans as usize))
            {
                if let Some(aggref) = pertrans.aggref.as_ref() {
                    return Some(aggref);
                }
            }
        }
    }
    None
}

/// `AggGetTempMemoryContext(fcinfo)` — the short-lived per-input-tuple memory
/// context an aggregate may use for scratch space, or `None`.
pub fn AggGetTempMemoryContext<'mcx>(
    fcinfo: &FunctionCallInfoBaseData<'mcx>,
) -> Option<MemoryContext> {
    if let Some(aggstate) = agg_context(fcinfo) {
        // return aggstate->tmpcontext->ecxt_per_tuple_memory. As in
        // AggCheckCallContext, C returns a pointer to the AggState-owned
        // context; the non-Copy owned `MemoryContext` handle can't be moved out
        // of the borrow — the handoff is the unported call-frame back-reference.
        let _ = aggstate.tmpcontext;
        panic!(
            "backend_executor_nodeAgg::AggGetTempMemoryContext: live-AggState \
             tmpcontext handoff — the tmpcontext is an EcxtId into the EState pool, \
             but this fmgr call-frame back-reference does not thread the EState"
        );
    }
    None
}

/// `AggStateIsShared(fcinfo)` — whether the current aggregate's transition
/// state value is shared between multiple Aggrefs (so a transfn must not
/// modify it in place). Returns `true` when not called as an aggregate
/// support function (the conservative "don't scribble on your input" answer).
pub fn AggStateIsShared<'mcx>(fcinfo: &FunctionCallInfoBaseData<'mcx>) -> bool {
    if let Some(aggstate) = agg_context(fcinfo) {
        // check curperagg (valid when in a final function)
        if aggstate.curperagg >= 0 {
            if let Some(peragg) = aggstate
                .peragg
                .as_ref()
                .and_then(|p| p.get(aggstate.curperagg as usize))
            {
                if let Some(pertrans) = aggstate
                    .pertrans
                    .as_ref()
                    .and_then(|p| p.get(peragg.transno as usize))
                {
                    return pertrans.aggshared;
                }
            }
        }
        // check curpertrans (valid when in a transition function)
        if aggstate.curpertrans >= 0 {
            if let Some(pertrans) = aggstate
                .pertrans
                .as_ref()
                .and_then(|p| p.get(aggstate.curpertrans as usize))
            {
                return pertrans.aggshared;
            }
        }
    }
    true
}

/// `AggRegisterCallback(fcinfo, func, arg)` — register a callback to be fired
/// when the aggregate's context is reset/deleted (used by aggregates with
/// internal state needing cleanup).
pub fn AggRegisterCallback<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    func: types_nodes::ExprContextCallbackFunction,
    arg: types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
) -> PgResult<()> {
    if agg_context(fcinfo).is_some() {
        // RegisterExprContextCallback(aggstate->curaggcontext, func, arg);
        // RegisterExprContextCallback is owned by executor/execUtils.c, and
        // registering against the live curaggcontext requires the &mut AggState
        // back-reference the trimmed call frame on main does not carry — so the
        // delegation panics through the unported execUtils owner.
        let _ = (func, arg);
        panic!(
            "backend_executor_execUtils::RegisterExprContextCallback: \
             unported (AggRegisterCallback delegation)"
        );
    }
    // elog(ERROR, "aggregate function cannot register a callback in this context")
    Err(types_error::PgError::error(
        "aggregate function cannot register a callback in this context",
    ))
}


/// `ExecAggEstimate(node, pcxt)` — estimate the DSM space for per-worker
/// aggregate instrumentation. Installed into `nodeAgg-pq-seams`.
pub fn ExecAggEstimate<'mcx>(
    node: &mut AggStateData<'mcx>,
    pcxt: ParallelContextHandle,
) -> PgResult<()> {
    // don't need this if not instrumenting or no workers
    let nworkers = parallel_seams::pcxt_nworkers::call(pcxt);
    if node.ss.ps.instrument.is_none() || nworkers == 0 {
        return Ok(());
    }

    // size = mul_size(pcxt->nworkers, sizeof(AggregateInstrumentation));
    // size = add_size(size, offsetof(SharedAggInfo, sinstrument));
    //
    // `SharedAggInfo` is a flexible-array-tail DSM object; the chunk byte count
    // is `offsetof(SharedAggInfo, sinstrument) + nworkers * sizeof(elem)`. Route
    // it through the keystone `estimate_flex` so the chunk-sizing call reads as
    // the keystone's flexible-array variant (BUFFERALIGN is applied by
    // `shm_toc_allocate`, exactly as for `store_fixed_state` today).
    let nbytes = (nworkers as usize)
        .checked_mul(core::mem::size_of::<AggregateInstrumentation>())
        .expect("mul_size overflow")
        .checked_add(shared_agg_info_sinstrument_offset())
        .expect("add_size overflow");
    let size = shared_dsm_object::estimate_flex(nbytes);
    let estimator = parallel_seams::pcxt_estimator::call(pcxt);
    parallel_seams::shm_toc_estimate_chunk::call(estimator, size);
    parallel_seams::shm_toc_estimate_keys::call(estimator, 1);
    Ok(())
}

/// `ExecAggInitializeDSM(node, pcxt)` — allocate the per-worker
/// instrumentation area in DSM and stash its pointer in `shared_info`.
pub fn ExecAggInitializeDSM<'mcx>(
    node: &mut AggStateData<'mcx>,
    pcxt: ParallelContextHandle,
) -> PgResult<()> {
    // don't need this if not instrumenting or no workers
    let nworkers = parallel_seams::pcxt_nworkers::call(pcxt);
    if node.ss.ps.instrument.is_none() || nworkers == 0 {
        return Ok(());
    }

    // size = offsetof(SharedAggInfo, sinstrument)
    //        + pcxt->nworkers * sizeof(AggregateInstrumentation);
    let size = shared_agg_info_sinstrument_offset()
        + (nworkers as usize) * core::mem::size_of::<AggregateInstrumentation>();

    // node->shared_info = shm_toc_allocate(pcxt->toc, size);
    // memset(node->shared_info, 0, size);  -> zeroed sinstrument slots
    // node->shared_info->num_workers = pcxt->nworkers;
    // shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id, node->shared_info);
    //
    // The leader `shm_toc_allocate`s the chunk (real keystone-backed call,
    // below) and would then `shared_dsm_object::place_and_init` a `repr(C)`
    // flexible-array `SharedAggInfo` over it (num_workers leader-write scalar +
    // a zeroed `AggregateInstrumentation sinstrument[]` whose per-worker slots
    // are launch-once-per-worker plain writes) and stash the resulting
    // `SharedRef` for the worker copyback / leader retrieve. Two surfaces are
    // genuinely missing for that handoff:
    //
    //  1. CARRIER: the live `SharedAggInfo *` lives in DSM; the AggState field
    //     `node->shared_info` is — on the already-merged nodeAgg contract — an
    //     in-process `PgBox<SharedAggInfo>` (types-nodes), which cannot hold the
    //     DSM `SharedRef`/chunk cursor. `SharedRef` is unstorable in `types-nodes`
    //     anyway (it lives in the `backend-access-transam-parallel` keystone
    //     crate; importing it into `types-nodes` inverts the crate layering).
    //     Re-typing `AggStateData.shared_info` to a DSM carrier is a
    //     contract-divergence from the merged nodeAgg port and would also force a
    //     rewrite of the worker copyback in `node_lifecycle::ExecEndAgg` (a
    //     sibling family's file).
    //  2. FAM ACCESSOR: the keystone exposes `place_and_init`/`attach`/`get`
    //     (whole-`T` placement + a shared `&T`) but no sanctioned per-element
    //     accessor for a flexible-array tail; reaching `sinstrument[i]` from a
    //     shared `&T` needs raw pointer arithmetic, which node code may not do
    //     (the only sanctioned raw-pointer surface is the keystone, and it does
    //     not yet offer a FAM-slot accessor — cf. `sei_plan_node_id` lives in the
    //     keystone crate itself, not in a node crate).
    //  3. plan_node_id: the DSM TOC key `node->ss.ps.plan->plan_node_id` is
    //     unreachable through the trimmed shared `Node` vocabulary (`ss.ps.plan`
    //     is `None`; the PlanState back-reference / Node->Agg resolution is
    //     unported — see node_lifecycle::agg_plan_node).
    //
    // The chunk allocation itself is a real keystone-backed shm_toc call; the
    // placement + carrier + key handoff mirror-and-panic into the parallel DSM
    // owner until those surfaces land.
    let toc = parallel_seams::pcxt_toc::call(pcxt);
    let chunk = parallel_seams::shm_toc_allocate::call(toc, size);
    let _ = (chunk, nworkers);
    panic!(
        "backend_access_transam_parallel::shared_dsm_object: SharedAggInfo DSM \
         place_and_init + carrier handoff (ExecAggInitializeDSM) — needs a \
         DSM-resident shared_info carrier (merged AggState uses in-process \
         PgBox<SharedAggInfo>; SharedRef is unstorable in types-nodes) and a \
         keystone flexible-array slot accessor; unported"
    );
}

/// `ExecAggInitializeWorker(node, pwcxt)` — in a worker, attach to the shared
/// instrumentation area.
pub fn ExecAggInitializeWorker<'mcx>(
    node: &mut AggStateData<'mcx>,
    pwcxt: ParallelWorkerContextHandle,
) -> PgResult<()> {
    // node->shared_info =
    //     shm_toc_lookup(pwcxt->toc, node->ss.ps.plan->plan_node_id, true);
    //
    // The worker `shm_toc_lookup`s the leader's chunk by plan_node_id and would
    // `shared_dsm_object::attach` to it, storing the `SharedRef` in
    // `node->shared_info` so its own `ExecEndAgg` copyback lands in the shared
    // DSM bytes. This is blocked on the SAME three surfaces as
    // ExecAggInitializeDSM: the DSM-resident `shared_info` carrier (merged
    // AggState holds an in-process `PgBox<SharedAggInfo>`; `SharedRef` is
    // unstorable in types-nodes), the keystone flexible-array slot accessor, and
    // the unported `plan_node_id` TOC key. Mirror-and-panic into the parallel
    // DSM owner until those land.
    let _ = (node, pwcxt);
    panic!(
        "backend_access_transam_parallel::shared_dsm_object: SharedAggInfo DSM \
         attach (ExecAggInitializeWorker) — needs a DSM-resident shared_info \
         carrier and the keystone flexible-array slot accessor; unported"
    );
}

/// `ExecAggRetrieveInstrumentation(node)` — in the leader, copy the
/// per-worker instrumentation out of DSM into the node's own storage.
pub fn ExecAggRetrieveInstrumentation<'mcx>(node: &mut AggStateData<'mcx>) -> PgResult<()> {
    // if (node->shared_info == NULL) return;
    let si = match node.shared_info.as_ref() {
        None => return Ok(()),
        Some(si) => si,
    };

    // size = offsetof(SharedAggInfo, sinstrument)
    //        + node->shared_info->num_workers * sizeof(AggregateInstrumentation);
    // si = palloc(size); memcpy(si, node->shared_info, size); node->shared_info = si;
    //
    // In the leader, C re-homes the DSM `SharedAggInfo` into private memory: it
    // would `shared_dsm_object::attach` to the carried chunk and copy each
    // `sinstrument[i]` slot out via the keystone flexible-array accessor. With
    // the merged in-process `PgBox<SharedAggInfo>` carrier, `node->shared_info`
    // is already private memory (no DSM round-trip happened — see
    // ExecAggInitializeDSM), so there is nothing real to copy out: the leader
    // never observed the workers' DSM slots. Faithfully closing this requires
    // the DSM-resident carrier + keystone flexible-array accessor that
    // InitializeDSM/Worker also need; mirror-and-panic until they land.
    let _ = si;
    panic!(
        "backend_access_transam_parallel::shared_dsm_object: SharedAggInfo DSM \
         copy-out (ExecAggRetrieveInstrumentation) — needs the DSM-resident \
         shared_info carrier and the keystone flexible-array slot accessor; \
         unported"
    );
}

/// `offsetof(SharedAggInfo, sinstrument)` — the byte offset of the flexible
/// `AggregateInstrumentation sinstrument[]` array past the leading
/// `int num_workers`, with the array's natural alignment. Mirrors the C
/// `offsetof` used in the DSM size estimate.
#[inline]
fn shared_agg_info_sinstrument_offset() -> usize {
    let align = core::mem::align_of::<AggregateInstrumentation>();
    // num_workers is a C `int`; round its size up to the array element's
    // alignment, exactly as the C struct layout pads before the FAM.
    let after_num_workers = core::mem::size_of::<i32>();
    after_num_workers.div_ceil(align) * align
}

// Keep the SharedAggInfo type referenced so its consumed shape is exercised by
// this family even while the DSM store/copy paths route through the owner.
#[allow(dead_code)]
fn _shared_agg_info_marker(_: &SharedAggInfo<'_>) {}

// ---------------------------------------------------------------------------
// Seam shims installed into `backend-executor-nodeAgg-pq-seams`.
//
// `execParallel` dispatches the per-node parallel hooks generically, holding a
// `PlanState *` (here the opaque [`PlanStateHandle`]); the C `ExecAggEstimate`
// etc. begin with the `(AggState *) node` cast. Recovering the live
// `AggStateData` from the handle is the executor's `PlanState`-pointer registry
// — that pointer-table is the unported executor surface, so each shim performs
// the C cast through `resolve_agg_state` (which panics until that registry
// lands) and then runs the real, ported entry point above.
// ---------------------------------------------------------------------------

/// `(AggState *) node` — recover the live `AggStateData` a `PlanStateHandle`
/// refers to. The executor's `PlanState` pointer registry that backs this
/// lookup is not yet ported.
fn resolve_agg_state<'mcx>(_node: PlanStateHandle) -> &'mcx mut AggStateData<'mcx> {
    panic!(
        "backend-executor-nodeAgg: resolving a PlanStateHandle to the live AggState needs the \
         executor PlanState pointer registry (unported); the (AggState *) node cast in the \
         ExecAgg* parallel hooks cannot run yet"
    );
}

/// Seam shim for `ExecAggEstimate`.
fn exec_agg_estimate_shim(node: PlanStateHandle, pcxt: ParallelContextHandle) -> PgResult<()> {
    ExecAggEstimate(resolve_agg_state(node), pcxt)
}

/// Seam shim for `ExecAggInitializeDSM`.
fn exec_agg_initialize_dsm_shim(
    node: PlanStateHandle,
    pcxt: ParallelContextHandle,
) -> PgResult<()> {
    ExecAggInitializeDSM(resolve_agg_state(node), pcxt)
}

/// Seam shim for `ExecAggInitializeWorker`.
fn exec_agg_initialize_worker_shim(
    node: PlanStateHandle,
    pwcxt: ParallelWorkerContextHandle,
) -> PgResult<()> {
    ExecAggInitializeWorker(resolve_agg_state(node), pwcxt)
}

/// Seam shim for `ExecAggRetrieveInstrumentation`.
fn exec_agg_retrieve_instrumentation_shim(node: PlanStateHandle) -> PgResult<()> {
    ExecAggRetrieveInstrumentation(resolve_agg_state(node))
}

/// Install the `aggapi` parallel-instrumentation seams this unit owns
/// (`backend-executor-nodeAgg-pq-seams`).
pub fn init_seams() {
    backend_executor_nodeAgg_pq_seams::exec_agg_estimate::set(exec_agg_estimate_shim);
    backend_executor_nodeAgg_pq_seams::exec_agg_initialize_dsm::set(exec_agg_initialize_dsm_shim);
    backend_executor_nodeAgg_pq_seams::exec_agg_initialize_worker::set(
        exec_agg_initialize_worker_shim,
    );
    backend_executor_nodeAgg_pq_seams::exec_agg_retrieve_instrumentation::set(
        exec_agg_retrieve_instrumentation_shim,
    );
}
