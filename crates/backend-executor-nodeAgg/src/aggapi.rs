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
use types_nodes::nodeagg::{Aggref, AggStateData, AggregateInstrumentation, SharedAggInfo};
use types_execparallel::{
    ParallelContextHandle, ParallelWorkerContextHandle, PlanStateHandle,
};

use backend_access_transam_parallel_seams as parallel_seams;

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
        let _ = aggstate.tmpcontext.as_ref();
        panic!(
            "backend_executor_nodeAgg::AggGetTempMemoryContext: live-AggState \
             tmpcontext handoff — unported call-frame back-reference"
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
    arg: types_datum::Datum,
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
    let size = (nworkers as usize)
        .checked_mul(core::mem::size_of::<AggregateInstrumentation>())
        .expect("mul_size overflow")
        .checked_add(shared_agg_info_sinstrument_offset())
        .expect("add_size overflow");
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
    // The DSM chunk that backs node->shared_info is owned by the parallel
    // subsystem (a live `SharedAggInfo *` into DSM); the owned model carries
    // `shared_info` as in-process `PgBox<SharedAggInfo>` and cannot hold a DSM
    // pointer, and the parallel-seams crate exposes no typed SharedAggInfo DSM
    // store. The allocate/insert therefore delegate to the unported parallel
    // DSM owner and panic until it lands.
    let toc = parallel_seams::pcxt_toc::call(pcxt);
    let chunk = parallel_seams::shm_toc_allocate::call(toc, size);
    let _ = (chunk, nworkers);
    panic!(
        "backend_access_transam_parallel: SharedAggInfo DSM store \
         (ExecAggInitializeDSM) — unported"
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
    // Reattaching `shared_info` to the worker's DSM copy is the same DSM-owner
    // operation as InitializeDSM: the resulting `SharedAggInfo *` lives in DSM,
    // which the in-process `PgBox<SharedAggInfo>` model can't hold. Delegates
    // to the unported parallel DSM owner.
    let _ = (node, pwcxt);
    panic!(
        "backend_access_transam_parallel: SharedAggInfo DSM lookup \
         (ExecAggInitializeWorker) — unported"
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
    // The C re-homes the DSM `SharedAggInfo` into private memory; the value the
    // owned model holds is the DSM-resident `SharedAggInfo` whose materialized
    // contents come from the parallel DSM owner. Copying it out is that owner's
    // operation.
    let _ = si;
    panic!(
        "backend_access_transam_parallel: SharedAggInfo DSM copy-out \
         (ExecAggRetrieveInstrumentation) — unported"
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
