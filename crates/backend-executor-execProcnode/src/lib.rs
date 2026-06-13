#![allow(non_snake_case)]
// Every fallible function returns the project-wide `PgResult` (== `Result<_,
// PgError>`); `PgError` is a large owned struct, so the un-boxed `Err` variant
// trips `clippy::result_large_err`. The un-boxed return is the project's error
// contract, so accept the lint crate-wide.
#![allow(clippy::result_large_err)]
// SCAFFOLD STAGE: the dispatch arms route to per-node-owner seams that are not
// all ported yet; the family bodies are `todo!()` until those owners land.
#![allow(dead_code)]
#![allow(unused_variables)]

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
//! [`PlanStateNode`]: types_nodes::PlanStateNode
//! [`EStateData`]: types_nodes::EStateData

pub mod execProcnode_init;
pub mod execProcnode_run_end;

/// Install every seam this unit owns.
///
/// The unit owns one seam crate (by C-source coverage of `execProcnode.c`):
/// `backend-executor-execProcnode-seams`. Every declaration in it is installed
/// here, exactly once.
pub fn init_seams() {
    use backend_executor_execProcnode_seams as seams;

    seams::exec_init_node::set(execProcnode_init::exec_init_node);
    seams::exec_proc_node::set(execProcnode_run_end::exec_proc_node);
    seams::exec_end_node::set(execProcnode_run_end::exec_end_node);
}
