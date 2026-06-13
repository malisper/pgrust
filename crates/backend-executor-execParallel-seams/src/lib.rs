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
