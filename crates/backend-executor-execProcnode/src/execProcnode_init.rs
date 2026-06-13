//! `execProcnode-init` family — node-tree initialization dispatch.
//!
//! Owns `ExecInitNode` (the 35-way `Plan`-tag switch that recursively builds
//! the plan-state tree by routing each `Plan` tag to the owning node unit's
//! `ExecInit*` routine, then runs the `initPlan` and instrumentation tail) and
//! `ExecSetExecProcNode` (installs the `ExecProcNode` callback wrapper).

use mcx::{Mcx, PgBox};
use types_error::PgResult;
use types_nodes::nodes::Node;
use types_nodes::{EStateData, PlanStateNode};

/// `ExecInitNode(node, estate, eflags)` (execProcnode.c).
///
/// Recursively initialize the plan subtree rooted at `node`, returning its
/// plan-state tree. A `None` plan yields `None` (C `if (node == NULL) return
/// NULL;`). After building the concrete state node via the owning node unit's
/// `ExecInit*` seam, the C code:
///   * `ExecSetExecProcNode(result, result->ExecProcNode)` — install the
///     first-call wrapper,
///   * walk `node->initPlan` building `SubPlanState`s via `ExecInitSubPlan`,
///   * if `estate->es_instrument`, attach `InstrAlloc` instrumentation.
///
/// The 35-way dispatch and the tail land at assembly, once the per-node-owner
/// `ExecInit*` seams (and the SubPlan/Instr seams) are available.
pub fn exec_init_node<'mcx>(
    mcx: Mcx<'mcx>,
    node: Option<&'mcx Node<'mcx>>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<Option<PgBox<'mcx, PlanStateNode<'mcx>>>> {
    todo!("execProcnode-init: ExecInitNode 35-way Plan-tag dispatch + initPlan/instrument tail")
}

/// `ExecSetExecProcNode(node, function)` (execProcnode.c).
///
/// Install a node's `ExecProcNode` callback behind the first-call wrapper:
/// C sets `node->ExecProcNodeReal = function` and `node->ExecProcNode =
/// ExecProcNodeFirst`. In the owned model the per-node "real" routine is the
/// owner's next-tuple seam; this records which one and arms the
/// first-execution wrapper (see `execProcnode_run_end::exec_proc_node`).
pub fn ExecSetExecProcNode<'mcx>(node: &mut PlanStateNode<'mcx>) {
    todo!("execProcnode-init: ExecSetExecProcNode — arm ExecProcNodeFirst wrapper")
}
