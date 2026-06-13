//! `execProcnode-run-end` family — run and teardown dispatch.
//!
//! Owns the execution-time dispatch trio plus their teardown counterparts:
//!   * `ExecProcNode` and its wrapper machinery `ExecProcNodeFirst` /
//!     `ExecProcNodeInstr` (next-tuple pull),
//!   * `MultiExecProcNode` (nodes that return a whole hashtable/bitmap rather
//!     than a tuple),
//!   * `ExecEndNode` (recursive teardown switch),
//!   * `ExecShutdownNode` (release async resources, via the
//!     `planstate_tree_walker` walk).

use mcx::PgBox;
use types_error::PgResult;
use types_nodes::nodes::Node;
use types_nodes::{EStateData, PlanStateNode, SlotId};

/// `ExecProcNode(node)` (executor.h / execProcnode.c).
///
/// Pull the next tuple from `node` by dispatching through its installed
/// `ExecProcNode` callback (the owner's next-tuple seam). On the first call
/// the C `ExecProcNodeFirst` wrapper runs `check_stack_depth()` and, if the
/// node is instrumented, swaps in `ExecProcNodeInstr` (which brackets the call
/// with `InstrStartNode`/`InstrStopNode`); otherwise it dispatches directly to
/// the "real" routine thereafter. Returns the produced tuple's [`SlotId`], or
/// `None` for the C `NULL` (TupIsNull) return.
pub fn exec_proc_node<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    todo!("execProcnode-run-end: ExecProcNode + ExecProcNodeFirst/ExecProcNodeInstr wrappers")
}

/// `MultiExecProcNode(node)` (execProcnode.c).
///
/// Execute a node that returns a whole result object rather than a tuple
/// (`T_HashState` → hashtable, `T_BitmapIndexScanState`/`T_BitmapAndState`/
/// `T_BitmapOrState` → bitmap). Does `check_stack_depth()`,
/// `CHECK_FOR_INTERRUPTS()`, an `ExecReScan` if `chgParam` changed, then the
/// 4-way `MultiExec*` dispatch. Returns the produced result `Node`; an
/// unrecognized tag is `elog(ERROR)`.
pub fn multi_exec_proc_node<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, Node<'mcx>>> {
    todo!("execProcnode-run-end: MultiExecProcNode 4-way dispatch")
}

/// `ExecEndNode(node)` (execProcnode.c).
///
/// Recursively clean up the plan-state subtree. A `None` node is a no-op (C
/// leaf guard). Frees `node->chgParam` if set, then runs the ~40-way teardown
/// switch routing each state tag to the owner's `ExecEnd*` seam (the
/// `T_ValuesScanState`/`T_NamedTuplestoreScanState`/`T_WorkTableScanState`
/// arms have no cleanup; an unrecognized tag is `elog(ERROR)`).
pub fn exec_end_node<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("execProcnode-run-end: ExecEndNode teardown dispatch")
}

/// `ExecShutdownNode(node)` (execProcnode.c).
///
/// Give execution nodes a chance to stop asynchronous resource consumption and
/// release held resources. C drives `ExecShutdownNode_walker` over the tree via
/// `planstate_tree_walker`: for a running instrumented node it brackets the
/// walk with `InstrStartNode`/`InstrStopNode(.., 0)`, and dispatches the
/// `T_GatherState`/`T_ForeignScanState`/`T_CustomScanState`/
/// `T_GatherMergeState`/`T_HashState`/`T_HashJoinState` arms to the owner's
/// `ExecShutdown*` seam.
pub fn exec_shutdown_node<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("execProcnode-run-end: ExecShutdownNode tree walk + shutdown dispatch")
}
