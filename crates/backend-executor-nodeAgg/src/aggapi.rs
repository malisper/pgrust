//! Aggregate-support-function API family: the functions an aggregate's
//! transition/final function may call to introspect its calling context
//! (`AggCheckCallContext` and friends), plus the parallel-instrumentation
//! entry points that move per-worker hash-agg metrics through DSM.
//!
//! The `fcinfo->context` of a support function points at the live `AggState`
//! (or a `WindowAggState`); these resolve it. The parallel entry points are
//! the methods this unit installs into `backend-executor-nodeAgg-pq-seams`.

use types_error::PgResult;
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_nodes::nodeagg::Aggref;
use types_nodes::EcxtId;
use crate::aggstate::{AggStateData, AggregateInstrumentation, SharedAggInfo};
use types_execparallel::{
    ParallelContextHandle, ParallelWorkerContextHandle, PlanStateHandle,
};

use backend_access_transam_parallel as parallel_seams;
use backend_access_transam_parallel::shared_dsm_object;

/// `AGG_CONTEXT_AGGREGATE` (executor/executor.h) — called as a plain aggregate.
pub const AGG_CONTEXT_AGGREGATE: i32 = 1;
/// `AGG_CONTEXT_WINDOW` (executor/executor.h) — called as a window aggregate.
pub const AGG_CONTEXT_WINDOW: i32 = 2;

/// The C `context` of an fmgr call frame is `fmNodePtr context` — a `Node *`
/// the executor sets to the live `AggState`/`WindowAggState`. This recovers it:
/// `if (fcinfo->context && IsA(fcinfo->context, AggState))` then
/// `(AggState *) fcinfo->context`, returning the concrete `AggStateData` when
/// the frame is an aggregate call and `None` otherwise (the C `NULL` /
/// not-an-`AggState` fall-through).
///
/// The back-reference is carried as the tag-checked
/// [`AggStateContextLink`](types_nodes::aggstate_carrier::AggStateContextLink)
/// inside [`FmgrCallContext::Agg`] (the `PlanStateLink` discipline); the
/// downcast to the concrete `AggStateData` is C's `(AggState *)` cast.
#[inline]
fn agg_context<'a, 'mcx>(
    fcinfo: &'a FunctionCallInfoBaseData<'mcx>,
) -> Option<&'a AggStateData<'mcx>> {
    // if (fcinfo->context && IsA(fcinfo->context, AggState)) {
    //     AggState *aggstate = (AggState *) fcinfo->context; ... }
    let live = fcinfo.context.as_ref()?.as_agg_state()?;
    types_nodes::aggstate_carrier::downcast_agg_state_ref::<AggStateData<'mcx>>(live)
}

/// `AggCheckCallContext(fcinfo, &aggcontext)` — report whether the function is
/// being called as an aggregate transition/final function. Returns
/// `AGG_CONTEXT_AGGREGATE` (1) / `AGG_CONTEXT_WINDOW` (2) / 0, and (when called
/// as an aggregate) the [`EcxtId`] of the appropriate aggregate `ExprContext`
/// (the owned-model rendering of C's `*aggcontext = ...->ecxt_per_tuple_memory`
/// out-parameter; see the type note below).
///
/// MODEL NOTE: C fills `*aggcontext` with the `MemoryContext` *pointer*
/// `aggstate->curaggcontext->ecxt_per_tuple_memory`. The owned `MemoryContext`
/// is a non-Copy domain handle owned by the EState's `ExprContext` pool — it
/// cannot be returned by value, and the caller wants to *switch into* it (which
/// in this repo is addressed by [`EcxtId`], the model's `ExprContext *`). So the
/// out-parameter is returned as the curaggcontext's `EcxtId`; resolving it to
/// the live per-tuple `Mcx` is the caller's `MemoryContextSwitchTo` (an
/// execUtils EState-pool lookup), exactly as C dereferences the returned
/// pointer. The load-bearing `i32` context-code is now fully resolved.
pub fn AggCheckCallContext<'mcx>(
    fcinfo: &FunctionCallInfoBaseData<'mcx>,
) -> (i32, Option<EcxtId>) {
    if let Some(aggstate) = agg_context(fcinfo) {
        // if (aggcontext) *aggcontext = aggstate->curaggcontext->ecxt_per_tuple_memory;
        // return AGG_CONTEXT_AGGREGATE;
        //
        // `aggstate->curaggcontext` is an index into `aggcontexts`; that element
        // is the `ExprContext *` (an `EcxtId` in the owned model). Hand it back
        // as the out-parameter.
        let aggcontext = aggstate
            .aggcontexts
            .as_ref()
            .and_then(|c| c.get(aggstate.curaggcontext as usize))
            .copied();
        return (AGG_CONTEXT_AGGREGATE, aggcontext);
    }
    // The WindowAggState arm of the C (`IsA(fcinfo->context, WindowAggState)`)
    // is not modeled here (the WindowAggState carrier is not in this enum yet);
    // with no AggState context the function falls through to the C
    // `*aggcontext = NULL; return 0;` tail.
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
/// context an aggregate may use for scratch space, or `None` when not called as
/// an aggregate.
///
/// MODEL NOTE: as in [`AggCheckCallContext`], C returns the *pointer*
/// `aggstate->tmpcontext->ecxt_per_tuple_memory`; the owned `MemoryContext` is a
/// non-Copy EState-pool handle, so the model hands back the tmpcontext's
/// [`EcxtId`] (the `ExprContext *`), which the caller resolves to the live `Mcx`
/// through the execUtils pool when it switches into it.
pub fn AggGetTempMemoryContext<'mcx>(
    fcinfo: &FunctionCallInfoBaseData<'mcx>,
) -> Option<EcxtId> {
    if let Some(aggstate) = agg_context(fcinfo) {
        // return aggstate->tmpcontext->ecxt_per_tuple_memory;
        return aggstate.tmpcontext;
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
    if let Some(aggstate) = agg_context(fcinfo) {
        // RegisterExprContextCallback(aggstate->curaggcontext, func, arg);
        //
        // The calling AggState is now reachable (the K1 fcinfo->context channel);
        // `aggstate->curaggcontext` is an `EcxtId` into the EState ExprContext
        // pool. The residual is the delegation target: `RegisterExprContextCallback`
        // (execUtils, ported) takes a `&mut ExprContext`, and reaching the live
        // pool `&mut` for that `EcxtId` from here needs an EState-pool register
        // seam keyed by `EcxtId` (none is declared yet, and this entry point
        // carries no `&mut EState`). That EState-pool mutable handoff — NOT the
        // call-frame context channel, which now works — is the remaining surface.
        let _curaggcontext: i32 = aggstate.curaggcontext;
        let _ = (func, arg);
        panic!(
            "backend_executor_execUtils::RegisterExprContextCallback: AggRegisterCallback \
             reaches the live AggState (K1 context channel works) but the delegation needs \
             an EState ExprContext-pool register seam keyed by the curaggcontext EcxtId \
             (unported; this entry point holds no &mut EState)"
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
    let nworkers = parallel_seams::pcxt_nworkers(pcxt);
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
    let estimator = parallel_seams::pcxt_estimator(pcxt);
    parallel_seams::shm_toc_estimate_chunk(estimator, size);
    parallel_seams::shm_toc_estimate_keys(estimator, 1);
    Ok(())
}

/// `ExecAggInitializeDSM(node, pcxt)` — allocate the per-worker
/// instrumentation area in DSM and stash its pointer in `shared_info`.
pub fn ExecAggInitializeDSM<'mcx>(
    node: &mut AggStateData<'mcx>,
    pcxt: ParallelContextHandle,
) -> PgResult<()> {
    // don't need this if not instrumenting or no workers
    let nworkers = parallel_seams::pcxt_nworkers(pcxt);
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
    let toc = parallel_seams::pcxt_toc(pcxt);
    let chunk = parallel_seams::shm_toc_allocate(toc, size);
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

#[cfg(test)]
mod k1_context_channel_tests {
    //! K1 (#324/#335): exercise the `fcinfo->context = (Node *) aggstate`
    //! channel — `agg_context()` recovering the live `AggState` through the
    //! `FmgrCallContext::Agg(AggStateContextLink)` back-reference, and the
    //! `Agg*` support functions reading it. Mirrors C's
    //! `IsA(fcinfo->context, AggState)` + `(AggState *) fcinfo->context`.
    use super::*;
    use mcx::{MemoryContext, PgVec};
    use types_nodes::aggstate_carrier::AggStateContextLink;
    use types_nodes::fmgr::FmgrCallContext;

    /// A frame whose `context` is the live AggState resolves as an aggregate
    /// call: `AggCheckCallContext` => AGG_CONTEXT_AGGREGATE + the curaggcontext
    /// EcxtId, `AggGetTempMemoryContext` => the tmpcontext EcxtId.
    #[test]
    fn agg_frame_resolves_back_to_aggstate() {
        let ctx = MemoryContext::new("k1-test");
        let mcx = ctx.mcx();

        // Construct a minimal live AggState (PgBox-stable in real code; a stack
        // local that outlives the frame here, satisfying the link invariant).
        let mut aggstate = AggStateData::new_in(mcx).unwrap();
        // aggstate->aggcontexts[0] = (ExprContext *) EcxtId(3); curaggcontext = 0.
        let mut aggcontexts: PgVec<'_, EcxtId> = PgVec::new_in(mcx);
        aggcontexts.push(EcxtId(3));
        aggstate.aggcontexts = Some(aggcontexts);
        aggstate.curaggcontext = 0;
        // aggstate->tmpcontext = (ExprContext *) EcxtId(7);
        aggstate.tmpcontext = Some(EcxtId(7));
        // Not in a transition/final fn: curperagg = curpertrans = -1.
        aggstate.curperagg = -1;
        aggstate.curpertrans = -1;

        // fcinfo->context = (Node *) aggstate;
        let link = AggStateContextLink::from_ref(
            &aggstate as &(dyn types_nodes::aggstate_carrier::AggStateLive<'_> + '_),
        );
        let mut fcinfo = FunctionCallInfoBaseData::default();
        fcinfo.context = Some(FmgrCallContext::Agg(link));

        // agg_context resolves to the same live AggState (tag-checked downcast).
        assert!(agg_context(&fcinfo).is_some(), "agg_context must recover the AggState");

        // AggCheckCallContext => AGG_CONTEXT_AGGREGATE + curaggcontext EcxtId.
        let (code, aggcxt) = AggCheckCallContext(&fcinfo);
        assert_eq!(code, AGG_CONTEXT_AGGREGATE);
        assert_eq!(aggcxt, Some(EcxtId(3)));

        // AggGetTempMemoryContext => the tmpcontext EcxtId.
        assert_eq!(AggGetTempMemoryContext(&fcinfo), Some(EcxtId(7)));

        // Not in a transition/final fn => AggGetAggref is None; AggStateIsShared
        // falls through to the conservative `true`.
        assert!(AggGetAggref(&fcinfo).is_none());
        assert!(AggStateIsShared(&fcinfo));
    }

    /// A frame with no context is the C `fcinfo->context == NULL` case: not an
    /// aggregate call.
    #[test]
    fn null_context_is_not_an_aggregate_call() {
        let fcinfo = FunctionCallInfoBaseData::default();
        assert!(agg_context(&fcinfo).is_none());
        assert_eq!(AggCheckCallContext(&fcinfo), (0, None));
        assert_eq!(AggGetTempMemoryContext(&fcinfo), None);
        assert!(AggGetAggref(&fcinfo).is_none());
        // Conservative "don't scribble on your input" answer when not an agg.
        assert!(AggStateIsShared(&fcinfo));
    }
}
