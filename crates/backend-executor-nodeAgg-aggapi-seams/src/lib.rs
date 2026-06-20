//! Seam declarations for the aggregate-support API (`AggCheckCallContext`,
//! `AggGetAggref`, `AggStateIsShared`, `AggRegisterCallback`) exposed
//! *downward* to the adt crates that implement aggregate transition/final
//! functions (the ordered-set aggregates in `orderedsetaggs.c` being the first
//! consumer).
//!
//! ## Why a seam
//!
//! These functions live in `backend-executor-nodeAgg` (`aggapi.rs`) and read
//! the live `AggState` through the concrete `AggStateData<'mcx>`. An adt crate
//! that implements an aggregate support function sits BELOW `nodeAgg` in the
//! dependency DAG and cannot call into it directly. The standard seam discipline
//! inverts that edge: this crate (a leaf the adt crates can depend on) DECLARES
//! the four entry points, and `backend-executor-nodeAgg::init_seams()` INSTALLS
//! their bodies.
//!
//! ## How the AggState is recovered
//!
//! C carries the live `AggState` as `fcinfo->context = (Node *) aggstate`. In
//! the owned model the executor's by-OID `function_call_invoke` dispatch builds
//! the callee call frame inside fmgr-core, so the AggState back-pointer rides a
//! thread-local channel deposited by the executor before dispatch
//! ([`types_fmgr::fmgr::AggCallContextGuard`]) and is read back onto the callee
//! frame's [`agg_context`](types_fmgr::FunctionCallInfoBaseData::agg_context).
//! The support function receives the low-level `types_fmgr`
//! [`FunctionCallInfoBaseData`] (what every fmgr-called builtin gets), so these
//! seams take `&FunctionCallInfoBaseData`; the installed bodies reconstruct the
//! `types_nodes` `AggStateContextLink` from the raw image and delegate to the
//! real `aggapi.rs` logic.
#![allow(unused_doc_comments)]
#![allow(non_snake_case)]

use types_error::PgResult;
use types_fmgr::FunctionCallInfoBaseData;
use types_nodes::nodeagg::Aggref;
use types_nodes::execnodes::{EcxtId, ExprContextCallbackFunction};

/// `AggGetAggref(fcinfo)` — return a copy (in `mcx`) of the `Aggref` being
/// evaluated, or `None` if not called as an aggregate support function. C
/// returns the live `Aggref *`; the seam hands back an `mcx`-arena `clone_in`
/// copy because the live `Aggref` is owned by the `AggState` above this crate
/// (the consumer only reads `aggkind`/`aggorder`/`args`/`aggdirectargs`).
seam_core::seam!(
    pub fn agg_get_aggref<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        fcinfo: &FunctionCallInfoBaseData,
    ) -> PgResult<Option<Aggref<'mcx>>>
);

/// `AggCheckCallContext(fcinfo, &aggcontext)` — report whether the function is
/// being called as an aggregate transition/final function. Returns
/// `AGG_CONTEXT_AGGREGATE` (1) / `AGG_CONTEXT_WINDOW` (2) / 0, and (when called
/// as an aggregate) the [`EcxtId`] of the per-group aggregate `ExprContext`
/// (`aggstate->curaggcontext` — the owned rendering of C's `*aggcontext`
/// out-parameter; the caller resolves it to the live `Mcx` to switch into).
seam_core::seam!(
    pub fn agg_check_call_context(
        fcinfo: &FunctionCallInfoBaseData,
    ) -> (i32, Option<EcxtId>)
);

/// `AggStateIsShared(fcinfo)` — whether the current aggregate's transition state
/// is shared between multiple Aggrefs (so a transfn must not modify it in
/// place). Conservatively `true` when not called as an aggregate support
/// function.
seam_core::seam!(
    pub fn agg_state_is_shared(fcinfo: &FunctionCallInfoBaseData) -> bool
);

/// `AggRegisterCallback(fcinfo, func, arg)` — register a cleanup callback to be
/// fired when the aggregate's per-group context is reset/deleted (ordered-set
/// aggregates use this to `tuplesort_end` their sort state at end of group).
seam_core::seam!(
    pub fn agg_register_callback<'mcx>(
        fcinfo: &mut FunctionCallInfoBaseData,
        func: ExprContextCallbackFunction,
        arg: types_tuple::Datum<'mcx>,
    ) -> PgResult<()>
);

/// `AGG_CONTEXT_AGGREGATE` (executor/executor.h) — re-exported for consumers.
pub const AGG_CONTEXT_AGGREGATE: i32 = 1;
/// `AGG_CONTEXT_WINDOW` (executor/executor.h).
pub const AGG_CONTEXT_WINDOW: i32 = 2;
