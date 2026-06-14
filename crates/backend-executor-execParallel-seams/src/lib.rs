//! Seam declarations for `backend-executor-execParallel`
//! (`executor/execParallel.c`) — the public entry points cyclic callers
//! (`nodeGather.c`, `nodeGatherMerge.c`) invoke.
//!
//! `execParallel` installs every one of these from its `init_seams()`.

#![allow(unused_doc_comments)]
#![allow(non_snake_case)]

use mcx::Mcx;
use types_error::PgResult;
use types_execparallel::{
    DsmSegmentHandle, EStateHandle, ParallelExecutorInfo, PlanStateHandle, ShmTocHandle,
    TuplesNeeded,
};
use types_nodes::bitmapset::Bitmapset;

/// `ExecInitParallelPlan(planstate, estate, sendParams, nworkers, tuples_needed)`.
seam_core::seam!(pub fn ExecInitParallelPlan<'mcx>(
    mcx: Mcx<'mcx>,
    planstate: PlanStateHandle,
    estate: EStateHandle,
    send_params: &Bitmapset,
    nworkers: i32,
    tuples_needed: TuplesNeeded,
) -> PgResult<ParallelExecutorInfo<'mcx>>);

/// `ExecParallelCreateReaders(pei)`.
seam_core::seam!(pub fn ExecParallelCreateReaders<'mcx>(
    mcx: Mcx<'mcx>,
    pei: &mut ParallelExecutorInfo<'mcx>,
) -> PgResult<()>);

/// `ExecParallelReinitialize(planstate, pei, sendParams)`.
seam_core::seam!(pub fn ExecParallelReinitialize<'mcx>(
    mcx: Mcx<'mcx>,
    planstate: PlanStateHandle,
    pei: &mut ParallelExecutorInfo<'mcx>,
    send_params: &Bitmapset,
) -> PgResult<()>);

/// `ExecParallelFinish(pei)`.
seam_core::seam!(pub fn ExecParallelFinish<'mcx>(pei: &mut ParallelExecutorInfo<'mcx>) -> PgResult<()>);

/// `ExecParallelCleanup(pei)`.
seam_core::seam!(pub fn ExecParallelCleanup<'mcx>(pei: &mut ParallelExecutorInfo<'mcx>) -> PgResult<()>);

/// `ParallelQueryMain(seg, toc)` — the worker entry point.
seam_core::seam!(pub fn ParallelQueryMain<'mcx>(
    mcx: Mcx<'mcx>,
    seg: DsmSegmentHandle,
    toc: ShmTocHandle,
) -> PgResult<()>);

/// `ExecInitParallelPlan(planstate, estate, sendParams, nworkers, tuples_needed)`
/// over the executor's **owned** plan-state tree (`&mut PlanStateNode`) rather
/// than a `PlanStateHandle`. The handle-space [`ExecInitParallelPlan`]
/// declaration above is the bridge used once a parallel-planstate registry
/// exists; the owned executor nodes (nodeGather / nodeGatherMerge) thread their
/// `outerPlanState` directly, so they call this variant. The owner serializes
/// the plan, sets up the DSM, and returns the leader's
/// [`ParallelExecutorInfo`]. Allocates / can `ereport(ERROR)`.
seam_core::seam!(pub fn exec_init_parallel_plan_owned<'mcx>(
    mcx: Mcx<'mcx>,
    planstate: &mut types_nodes::PlanStateNode<'mcx>,
    estate: &mut types_nodes::EStateData<'mcx>,
    send_params: Option<&Bitmapset>,
    nworkers: i32,
    tuples_needed: TuplesNeeded,
) -> PgResult<ParallelExecutorInfo<'mcx>>);

/// `ExecParallelReinitialize(planstate, pei, sendParams)` over the owned
/// plan-state tree (re-initialize the DSM for a rescan). Owned-space companion
/// of [`ExecParallelReinitialize`]; see [`exec_init_parallel_plan_owned`].
seam_core::seam!(pub fn exec_parallel_reinitialize_owned<'mcx>(
    mcx: Mcx<'mcx>,
    planstate: &mut types_nodes::PlanStateNode<'mcx>,
    pei: &mut ParallelExecutorInfo<'mcx>,
    send_params: Option<&Bitmapset>,
) -> PgResult<()>);
