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
use types_nodes::nodeagg::{Aggref, AggStateData};
use types_execparallel::{ParallelContextHandle, ParallelWorkerContextHandle};

/// `AggCheckCallContext(fcinfo, &aggcontext)` — report whether the function is
/// being called as an aggregate transition/final function. Returns
/// `AGG_CONTEXT_AGGREGATE` (1) / `AGG_CONTEXT_WINDOW` (2) / 0, and (when not
/// null) the appropriate aggregate memory context.
pub fn AggCheckCallContext<'mcx>(
    fcinfo: &FunctionCallInfoBaseData<'mcx>,
) -> (i32, Option<MemoryContext>) {
    todo!("decomp")
}

/// `AggGetAggref(fcinfo)` — return the `Aggref` being evaluated, or `None` if
/// the function is not being called as an aggregate.
pub fn AggGetAggref<'a, 'mcx>(
    fcinfo: &'a FunctionCallInfoBaseData<'mcx>,
) -> Option<&'a Aggref<'mcx>> {
    todo!("decomp")
}

/// `AggGetTempMemoryContext(fcinfo)` — the short-lived per-input-tuple memory
/// context an aggregate may use for scratch space, or `None`.
pub fn AggGetTempMemoryContext<'mcx>(
    fcinfo: &FunctionCallInfoBaseData<'mcx>,
) -> Option<MemoryContext> {
    todo!("decomp")
}

/// `AggStateIsShared(fcinfo)` — whether the current aggregate's transition
/// state value is shared between multiple Aggrefs (so a transfn must not
/// modify it in place).
pub fn AggStateIsShared<'mcx>(fcinfo: &FunctionCallInfoBaseData<'mcx>) -> bool {
    todo!("decomp")
}

/// `AggRegisterCallback(fcinfo, func, arg)` — register a callback to be fired
/// when the aggregate's context is reset/deleted (used by aggregates with
/// internal state needing cleanup).
pub fn AggRegisterCallback<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    func: types_nodes::ExprContextCallbackFunction,
    arg: types_datum::Datum,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecAggEstimate(node, pcxt)` — estimate the DSM space for per-worker
/// aggregate instrumentation. Installed into `nodeAgg-pq-seams`.
pub fn ExecAggEstimate<'mcx>(
    node: &mut AggStateData<'mcx>,
    pcxt: ParallelContextHandle,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecAggInitializeDSM(node, pcxt)` — allocate the per-worker
/// instrumentation area in DSM and stash its pointer in `shared_info`.
pub fn ExecAggInitializeDSM<'mcx>(
    node: &mut AggStateData<'mcx>,
    pcxt: ParallelContextHandle,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecAggInitializeWorker(node, pwcxt)` — in a worker, attach to the shared
/// instrumentation area.
pub fn ExecAggInitializeWorker<'mcx>(
    node: &mut AggStateData<'mcx>,
    pwcxt: ParallelWorkerContextHandle,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecAggRetrieveInstrumentation(node)` — in the leader, copy the
/// per-worker instrumentation out of DSM into the node's own storage.
pub fn ExecAggRetrieveInstrumentation<'mcx>(node: &mut AggStateData<'mcx>) -> PgResult<()> {
    todo!("decomp")
}
