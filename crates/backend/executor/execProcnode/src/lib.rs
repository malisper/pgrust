#![allow(non_snake_case)]
// Every fallible function returns the project-wide `PgResult` (== `Result<_,
// PgError>`); `PgError` is a large owned struct, so the un-boxed `Err` variant
// trips `clippy::result_large_err`. The un-boxed return is the project's error
// contract, so accept the lint crate-wide.
#![allow(clippy::result_large_err)]
// SCAFFOLD STAGE: the dispatch arms route to per-node-owner seams that are not
// all ported yet; the unported-owner arms loud-panic until those owners land.
#![allow(dead_code)]
#![allow(unused_variables)]
// Until the per-node `ExecInit*` owners land, every `ExecInitNode` dispatch
// arm panics (diverges), so the tail after the dispatch `match` is statically
// unreachable. The tail is the faithful C logic and becomes reachable as the
// node owners' `ExecInit*` seams are wired in; accept the lint meanwhile.
#![allow(unreachable_code)]

//! `backend-executor-execProcnode` — port of `src/backend/executor/execProcnode.c`.
//!
//! execProcnode.c is the executor's node-dispatch layer: a thin set of switch
//! statements over `nodeTag(node)` that fan out to the per-node `ExecInit*`,
//! `Exec*` (next-tuple), `MultiExec*`, `ExecEnd*` and `ExecShutdown*` routines
//! owned by each `node*.c`. It owns almost no logic of its own — its job is the
//! dispatch and the `ExecProcNode` wrapper machinery (`ExecSetExecProcNode`,
//! `ExecProcNodeFirst`, `ExecProcNodeInstr`).
//!
//! In the owned model the C `PlanState *` is the [`PlanStateNode`] tagged enum,
//! so each `castNode`/function-pointer dispatch becomes a `match` arm, and each
//! arm calls into the owning node unit through that owner's per-node `-seams`
//! crate (a loud panic until the owner lands). The C back-pointer
//! `PlanState.state` is replaced by threading `&mut EStateData` explicitly.
//!
//! This unit is split into two family modules (decomposition track):
//!
//!  * [`execProcnode_init`] — `ExecInitNode` (the 35-way `Plan`-tag dispatch
//!    routing each arm to its per-node-owner `ExecInit*` seam, plus the
//!    `initPlan`/instrumentation tail) and `ExecSetExecProcNode`.
//!  * [`execProcnode_run_end`] — the run/teardown dispatch: `ExecProcNode`
//!    (with the `ExecProcNodeFirst`/`ExecProcNodeInstr` wrapper machinery),
//!    `MultiExecProcNode`, `ExecEndNode` and `ExecShutdownNode`.
//!
//! [`PlanStateNode`]: nodes::PlanStateNode
//! [`EStateData`]: nodes::EStateData

pub mod execProcnode_init;
pub mod execProcnode_run_end;
mod cte_seams;
mod lockrows_seams;
mod tidrangescan_seams;
mod samplescan_seams;

/// Install every seam this unit owns that corresponds to an `execProcnode.c`
/// function.
///
/// The unit owns `backend-executor-execProcnode-seams`. The declarations that
/// map to an `execProcnode.c` C function are installed here, exactly once:
/// `exec_init_node` (`ExecInitNode`), `exec_proc_node` (`ExecProcNode`),
/// `exec_end_node` (`ExecEndNode`), `multi_exec_proc_node`
/// (`MultiExecProcNode`) and `exec_set_tuple_bound` (`ExecSetTupleBound`).
///
/// The remaining declarations in that crate
/// (`mark_param_execplan_pending`/`clear_param_execplan`/
/// `param_execplan_pending`/`exec_set_param_plan_for_pending`/
/// `link_subplan_planstate`) are *not* `execProcnode.c` functions: the
/// `nodeSubplan` port parked these executor PARAM_EXEC / `es_subplanstates`
/// plumbing seams in this crate. Their bodies operate on the `ParamExecData`
/// `execPlan` link (not modeled on the trimmed struct) and `es_subplanstates`,
/// which belong to the executor's param/initplan machinery (execMain), not to
/// the node-dispatch layer; they stay uninstalled here pending that owner.
pub fn init_seams() {
    use execProcnode_seams as seams;

    seams::exec_init_node::set(execProcnode_init::exec_init_node);
    seams::exec_proc_node::set(execProcnode_run_end::exec_proc_node);
    seams::exec_end_node::set(execProcnode_run_end::exec_end_node);
    seams::exec_shutdown_node::set(execProcnode_run_end::exec_shutdown_node);
    seams::multi_exec_proc_node::set(execProcnode_run_end::multi_exec_proc_node);
    seams::exec_set_tuple_bound::set(execProcnode_run_end::exec_set_tuple_bound);

    // The CteScan leader-aliased `cte_*` family (declared in execMain-seams):
    // this dispatch crate owns the `ExecInitCteScan` call site and runs the CTE
    // subplan via `exec_proc_node`, so it installs the owned-model bodies here
    // (the shared per-CTE store lives in `EState.es_cte_shared`).
    cte_seams::init_seams();

    // The LockRows (FOR UPDATE/SHARE) node seams: this dispatch crate owns the
    // ExecInitLockRows/ExecProcNode call sites and the execTuples/execUtils/
    // tableam substrate the node reaches, so it installs the node's 24 seams
    // here (the EvalPlanQual recheck leg loud-errors — see lockrows_seams.rs).
    lockrows_seams::init_seams();

    // The TidRangeScan (WHERE ctid >= ... AND ctid < ...) node seams: this
    // dispatch crate owns the ExecInitTidRangeScan/ExecProcNode call sites and the
    // execTuples/execUtils/execExpr/tableam substrate the node reaches, so it
    // installs the node's seams here (the `exec_assign_scan_projection_info` and
    // `exec_scan_rescan` seams are installed by execScan, which owns those drivers).
    tidrangescan_seams::init_seams();

    // The SampleScan (TABLESAMPLE) node seams: this dispatch crate owns the
    // ExecInitSampleScan/ExecProcNode call sites and the execTuples/execUtils/
    // execExpr/tableam/prng/hash substrate the node reaches, so it installs the
    // node's seams here (the `exec_assign_scan_projection_info` and
    // `exec_scan_rescan` seams are installed by execScan, which owns those drivers).
    samplescan_seams::init_seams();
}
