//! Seam declarations for `backend-executor-execParallel`
//! (`executor/execParallel.c`) — the public entry points cyclic callers
//! (`nodeGather.c`, `nodeGatherMerge.c`) invoke.
//!
//! After #169 these are all driven over the executor's **owned** plan-state
//! tree (`&mut PlanStateNode`) and `EState` (`&mut EStateData`); the
//! handle-space entry points were retired. `execParallel` installs every one of these from its
//! `init_seams()`.

#![allow(unused_doc_comments)]
#![allow(non_snake_case)]

use ::mcx::Mcx;
use ::types_error::PgResult;
use execparallel::{DsmSegmentHandle, ParallelExecutorInfo, ShmTocHandle, TuplesNeeded};
use ::nodes::bitmapset::Bitmapset;

/// `ExecParallelCreateReaders(pei)`.
seam_core::seam!(pub fn ExecParallelCreateReaders<'mcx>(
    mcx: Mcx<'mcx>,
    pei: &mut ParallelExecutorInfo<'mcx>,
) -> PgResult<()>);

/// `ExecParallelFinish(pei)`.
seam_core::seam!(pub fn ExecParallelFinish<'mcx>(pei: &mut ParallelExecutorInfo<'mcx>) -> PgResult<()>);

/// `ExecParallelCleanup(pei)` — over the owned plan-state tree. `planstate` (the
/// leader's `outerPlanState`) is threaded in by `&mut` rather than carried in
/// `pei`: `pei` lives inside the Gather/GatherMerge node-state and the planstate
/// is a sibling field, so storing it in `pei` would be a self-borrow.
seam_core::seam!(pub fn ExecParallelCleanup<'mcx>(
    mcx: Mcx<'mcx>,
    pei: &mut ParallelExecutorInfo<'mcx>,
    planstate: &mut ::nodes::PlanStateNode<'mcx>,
) -> PgResult<()>);

/// `ParallelQueryMain(seg, toc)` — the worker entry point.
seam_core::seam!(pub fn ParallelQueryMain<'mcx>(
    mcx: Mcx<'mcx>,
    seg: DsmSegmentHandle,
    toc: ShmTocHandle,
) -> PgResult<()>);

/// `ExecInitParallelPlan(planstate, estate, sendParams, nworkers, tuples_needed)`
/// over the executor's **owned** plan-state tree (`&mut PlanStateNode`) and
/// `EState` (`&mut EStateData`). The owner serializes the plan, sets up the DSM,
/// and returns the leader's [`ParallelExecutorInfo`]. Allocates / can
/// `ereport(ERROR)`.
seam_core::seam!(pub fn exec_init_parallel_plan_owned<'mcx>(
    mcx: Mcx<'mcx>,
    planstate: &mut ::nodes::PlanStateNode<'mcx>,
    estate: &mut ::nodes::EStateData<'mcx>,
    send_params: Option<&Bitmapset<'mcx>>,
    nworkers: i32,
    tuples_needed: TuplesNeeded,
) -> PgResult<ParallelExecutorInfo<'mcx>>);

/// `ExecParallelReinitialize(planstate, pei, sendParams)` over the owned
/// plan-state tree (re-initialize the DSM for a rescan). `estate` (=
/// `planstate->state`) is threaded in by the caller.
seam_core::seam!(pub fn exec_parallel_reinitialize_owned<'mcx>(
    mcx: Mcx<'mcx>,
    planstate: &mut ::nodes::PlanStateNode<'mcx>,
    estate: &mut ::nodes::EStateData<'mcx>,
    pei: &mut ParallelExecutorInfo<'mcx>,
    send_params: Option<&Bitmapset<'mcx>>,
) -> PgResult<()>);
