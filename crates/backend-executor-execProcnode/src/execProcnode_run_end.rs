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

extern crate alloc;

use alloc::format;

use mcx::PgBox;
use types_error::{PgError, PgResult};
use types_nodes::nodes::Node;
use types_nodes::{EStateData, PlanStateNode, SlotId};

use backend_executor_execAmi_seams as execAmi;
use backend_executor_instrument_seams as instrument;
use backend_nodes_core_seams as nodes_core;
use backend_tcop_postgres_seams as tcop_postgres;

/// `ExecProcNode(node)` (executor.h / execProcnode.c).
///
/// Pull the next tuple from `node` by dispatching through its installed
/// `ExecProcNode` callback (the owner's next-tuple seam). On the first call
/// the C `ExecProcNodeFirst` wrapper runs `check_stack_depth()` and, if the
/// node is instrumented, swaps in `ExecProcNodeInstr` (which brackets the call
/// with `InstrStartNode`/`InstrStopNode`); otherwise it dispatches directly to
/// the "real" routine thereafter. Returns the produced tuple's [`SlotId`], or
/// `None` for the C `NULL` (TupIsNull) return.
///
/// C `ExecProcNode` is the `executor.h` inline macro `node->ExecProcNode(node)`
/// — it simply invokes the function pointer currently installed on the node.
/// `ExecSetExecProcNode` (the init family) arms that pointer with
/// [`exec_proc_node_first`] at init time, so the first call routes through the
/// first-execution wrapper below.
pub fn exec_proc_node<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // return node->ExecProcNode(node);
    let cb = node.ps_head().ExecProcNode.expect(
        "ExecProcNode called on a node whose ExecProcNode callback was never installed \
         (ExecSetExecProcNode not run)",
    );
    cb(node, estate)
}

/// `ExecProcNodeFirst(node)` (execProcnode.c, static).
///
/// `ExecProcNode` wrapper that performs the one-time stack-depth check, then
/// re-points the node's callback at either [`exec_proc_node_instr`] (when the
/// node is instrumented) or the node's "real" routine, and dispatches.
pub fn exec_proc_node_first<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // Perform stack depth check during the first execution of the node.
    tcop_postgres::check_stack_depth::call()?;

    // If instrumentation is required, change the wrapper to one that just does
    // instrumentation.  Otherwise we can dispense with all wrappers and have
    // ExecProcNode() directly call the relevant function from now on.
    //   if (node->instrument)
    //       node->ExecProcNode = ExecProcNodeInstr;
    //   else
    //       node->ExecProcNode = node->ExecProcNodeReal;
    if node.ps_head().instrument.is_some() {
        node.ps_head_mut().ExecProcNode = Some(exec_proc_node_instr);
    } else {
        node.ps_head_mut().ExecProcNode = node.ps_head().ExecProcNodeReal;
    }

    // return node->ExecProcNode(node);
    let cb = node.ps_head().ExecProcNode.expect(
        "ExecProcNodeFirst: node ExecProcNode callback missing after first-call rearm",
    );
    cb(node, estate)
}

/// `ExecProcNodeInstr(node)` (execProcnode.c, static).
///
/// `ExecProcNode` wrapper that brackets the real routine with
/// `InstrStartNode`/`InstrStopNode`. The counted tuple count is `0.0` when the
/// real routine returns no tuple (the C `TupIsNull(result) ? 0.0 : 1.0`),
/// else `1.0`.
pub fn exec_proc_node_instr<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // InstrStartNode(node->instrument);
    {
        let instr = node
            .ps_head_mut()
            .instrument
            .as_mut()
            .expect("ExecProcNodeInstr: node->instrument is NULL");
        instrument::instr_start_node::call(instr)?;
    }

    // result = node->ExecProcNodeReal(node);
    let cb = node.ps_head().ExecProcNodeReal.expect(
        "ExecProcNodeInstr: node ExecProcNodeReal callback missing",
    );
    let result = cb(node, estate)?;

    // InstrStopNode(node->instrument, TupIsNull(result) ? 0.0 : 1.0);
    {
        let n_tuples = if result.is_none() { 0.0 } else { 1.0 };
        let instr = node
            .ps_head_mut()
            .instrument
            .as_mut()
            .expect("ExecProcNodeInstr: node->instrument is NULL");
        instrument::instr_stop_node::call(instr, n_tuples)?;
    }

    Ok(result)
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
    // check_stack_depth();
    tcop_postgres::check_stack_depth::call()?;

    // CHECK_FOR_INTERRUPTS();
    tcop_postgres::check_for_interrupts::call()?;

    // if (node->chgParam != NULL) ExecReScan(node);
    if node.ps_head().chgParam.is_some() {
        execAmi::exec_re_scan::call(node, estate)?;
    }

    // switch (nodeTag(node)) — only node types that actually support multiexec
    // are listed. Each arm runs the owning node unit's `MultiExec*` routine,
    // which is reached through that owner's per-node seam (loud panic until the
    // owner lands). None of the multiexec owners
    // (`nodeHash`/`nodeBitmapIndexscan`/`nodeBitmapAnd`/`nodeBitmapOr`) expose a
    // `Node`-returning `MultiExecProcNode` seam yet, and their state variants
    // (`T_HashState` aside) are not yet present in `PlanStateNode`; mirror the C
    // switch and seam-and-panic per recognized arm.
    match node.tag() {
        // case T_HashState: result = MultiExecHash((HashState *) node);
        types_nodes::execstate_tags::T_HashState => {
            panic!(
                "MultiExecProcNode(T_HashState): route to backend-executor-nodeHash \
                 MultiExecHash seam (owner has not yet exposed a Node-returning \
                 MultiExecProcNode seam)"
            )
        }
        // case T_BitmapIndexScanState:
        //     result = MultiExecBitmapIndexScan((BitmapIndexScanState *) node);
        // case T_BitmapAndState: result = MultiExecBitmapAnd((BitmapAndState *) node);
        // case T_BitmapOrState:  result = MultiExecBitmapOr((BitmapOrState *) node);
        //
        // These three state types are not yet present in `PlanStateNode`, so
        // their tags cannot occur. When their executor units land they take a
        // `nodeTag` arm here routed through the owner's `MultiExec*` seam.
        //
        // default: elog(ERROR, "unrecognized node type: %d", (int) nodeTag(node));
        other => Err(PgError::error(format!(
            "unrecognized node type: {}",
            other.0 as i32
        ))),
    }
}

/// `ExecEndNode(node)` (execProcnode.c).
///
/// Recursively clean up the plan-state subtree. A `None` node is a no-op (C
/// leaf guard). Frees `node->chgParam` if set, then runs the ~40-way teardown
/// switch routing each state tag to the owner's `ExecEnd*` seam (the
/// `T_ValuesScanState`/`T_NamedTuplestoreScanState`/`T_WorkTableScanState`
/// arms have no cleanup; an unrecognized tag is `elog(ERROR)`).
///
/// The `None` (C `node == NULL`) leaf guard is handled by callers: the typed
/// `PlanStateNode` reference is always non-NULL here, mirroring how the C call
/// sites that descend into optional children (`if (child) ExecEndNode(child)`)
/// elide the recursion when the child is absent.
pub fn exec_end_node<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Make sure there's enough stack available.
    tcop_postgres::check_stack_depth::call()?;

    // if (node->chgParam != NULL) { bms_free(node->chgParam); node->chgParam = NULL; }
    if node.ps_head().chgParam.is_some() {
        let chg = node.ps_head_mut().chgParam.take();
        nodes_core::bms_free::call(chg);
    }

    // switch (nodeTag(node)) — route each concrete state to its owning node
    // unit's `ExecEnd*` routine. Each owner's teardown takes its concrete state
    // struct + `&mut EStateData`; it is reached through that owner's per-node
    // seam (loud panic until the owner exposes and wires it). The C `default`
    // arm is `elog(ERROR, "unrecognized node type")`. The owned `PlanStateNode`
    // enum is `#[non_exhaustive]` and currently carries only the variants whose
    // executor units have landed, so the remaining C arms cannot occur yet.
    match node {
        // case T_AppendState: ExecEndAppend((AppendState *) node);
        PlanStateNode::Append(_) => panic!(
            "ExecEndNode(T_AppendState): route to backend-executor-nodeAppend ExecEndAppend seam"
        ),
        // case T_MaterialState: ExecEndMaterial((MaterialState *) node);
        PlanStateNode::Material(_) => panic!(
            "ExecEndNode(T_MaterialState): route to backend-executor-nodeMaterial \
             ExecEndMaterial seam"
        ),
        // case T_MergeAppendState: ExecEndMergeAppend((MergeAppendState *) node);
        PlanStateNode::MergeAppend(_) => panic!(
            "ExecEndNode(T_MergeAppendState): route to backend-executor-nodeMergeappend \
             ExecEndMergeAppend seam"
        ),
        // case T_MergeJoinState: ExecEndMergeJoin((MergeJoinState *) node);
        PlanStateNode::MergeJoin(_) => panic!(
            "ExecEndNode(T_MergeJoinState): route to backend-executor-nodeMergejoin \
             ExecEndMergeJoin seam"
        ),
        // case T_MemoizeState: ExecEndMemoize((MemoizeState *) node);
        PlanStateNode::Memoize(_) => panic!(
            "ExecEndNode(T_MemoizeState): route to backend-executor-nodeMemoize \
             ExecEndMemoize seam"
        ),
        // case T_IndexOnlyScanState: ExecEndIndexOnlyScan((IndexOnlyScanState *) node);
        PlanStateNode::IndexOnlyScan(_) => panic!(
            "ExecEndNode(T_IndexOnlyScanState): route to backend-executor-nodeIndexonlyscan \
             ExecEndIndexOnlyScan seam"
        ),
        // case T_LimitState: ExecEndLimit((LimitState *) node);
        PlanStateNode::Limit(_) => panic!(
            "ExecEndNode(T_LimitState): route to backend-executor-nodeLimit ExecEndLimit seam"
        ),
        // case T_SortState: ExecEndSort((SortState *) node);
        PlanStateNode::Sort(_) => panic!(
            "ExecEndNode(T_SortState): route to backend-executor-nodeSort ExecEndSort seam"
        ),
        // case T_TableFuncScanState: ExecEndTableFuncScan((TableFuncScanState *) node);
        PlanStateNode::TableFuncScan(_) => panic!(
            "ExecEndNode(T_TableFuncScanState): route to backend-executor-nodeTablefuncscan \
             ExecEndTableFuncScan seam"
        ),
        // case T_NestLoopState: ExecEndNestLoop((NestLoopState *) node);
        PlanStateNode::NestLoop(_) => panic!(
            "ExecEndNode(T_NestLoopState): route to backend-executor-nodeNestloop \
             ExecEndNestLoop seam"
        ),
        // case T_HashJoinState: ExecEndHashJoin((HashJoinState *) node);
        PlanStateNode::HashJoin(_) => panic!(
            "ExecEndNode(T_HashJoinState): route to backend-executor-nodeHashjoin \
             ExecEndHashJoin seam"
        ),
        // case T_SeqScanState: ExecEndSeqScan((SeqScanState *) node);
        PlanStateNode::SeqScan(_) => panic!(
            "ExecEndNode(T_SeqScanState): route to backend-executor-nodeSeqscan ExecEndSeqScan seam"
        ),
        // case T_ForeignScanState: ExecEndForeignScan((ForeignScanState *) node);
        PlanStateNode::ForeignScan(_) => panic!(
            "ExecEndNode(T_ForeignScanState): route to backend-executor-nodeForeignscan \
             ExecEndForeignScan seam"
        ),
        // case T_HashState: ExecEndHash((HashState *) node);
        PlanStateNode::Hash(_) => panic!(
            "ExecEndNode(T_HashState): route to backend-executor-nodeHash ExecEndHash seam"
        ),

        // The remaining C arms (control nodes T_ResultState/T_ProjectSetState/
        // T_ModifyTableState/T_RecursiveUnionState/T_BitmapAndState/
        // T_BitmapOrState; scan nodes T_SampleScanState/T_GatherState/
        // T_GatherMergeState/T_IndexScanState/T_BitmapIndexScanState/
        // T_BitmapHeapScanState/T_TidScanState/T_TidRangeScanState/
        // T_SubqueryScanState/T_FunctionScanState/T_CteScanState/
        // T_CustomScanState; materialization nodes T_IncrementalSortState/
        // T_GroupState/T_AggState/T_WindowAggState/T_UniqueState/T_SetOpState/
        // T_LockRowsState; and the no-cleanup T_ValuesScanState/
        // T_NamedTuplestoreScanState/T_WorkTableScanState arms) are not yet
        // present in the `#[non_exhaustive]` `PlanStateNode` enum, so their tags
        // cannot occur. The C `default: elog(ERROR, "unrecognized node type")`
        // covers any tag with no arm.
        other => Err(PgError::error(format!(
            "unrecognized node type: {}",
            other.tag().0 as i32
        ))),
    }
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
///
/// C: `(void) ExecShutdownNode_walker(node, NULL);`
pub fn exec_shutdown_node<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    exec_shutdown_node_walker(node, estate)?;
    Ok(())
}

/// `ExecShutdownNode_walker(node, context)` (execProcnode.c, static).
///
/// The `planstate_tree_walker` callback. A `None` node returns `false`
/// (handled by callers that elide the recursion for absent children); for a
/// present node it does the stack-depth check, optionally brackets the
/// child-walk and shutdown dispatch with instrumentation start/stop, recurses
/// over the children, then dispatches the per-node `ExecShutdown*`.
///
/// The C return value is always `false` (the walker never stops early); the
/// owned model returns `PgResult<bool>` to surface the failure of any nested
/// `ereport(ERROR)`.
fn exec_shutdown_node_walker<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // check_stack_depth();
    tcop_postgres::check_stack_depth::call()?;

    // The full C body brackets the child-walk and per-node `ExecShutdown*`
    // dispatch with instrumentation start/stop:
    //
    //   if (node->instrument && node->instrument->running)
    //       InstrStartNode(node->instrument);
    //   planstate_tree_walker(node, ExecShutdownNode_walker, context);
    //   switch (nodeTag(node)) {
    //       case T_GatherState:      ExecShutdownGather(..);      break;
    //       case T_ForeignScanState: ExecShutdownForeignScan(..); break;
    //       case T_CustomScanState:  ExecShutdownCustomScan(..);  break;
    //       case T_GatherMergeState: ExecShutdownGatherMerge(..); break;
    //       case T_HashState:        ExecShutdownHash(..);        break;
    //       case T_HashJoinState:    ExecShutdownHashJoin(..);    break;
    //       default: break;
    //   }
    //   if (node->instrument && node->instrument->running)
    //       InstrStopNode(node->instrument, 0);
    //   return false;
    //
    // The recursion uses the typed `planstate_tree_walker` over `PlanStateNode`
    // (the `outerPlanState`/`innerPlanState`/per-node child-list walk), and the
    // dispatch arms call each node owner's `ExecShutdown*` routine. Neither the
    // typed walker (only the opaque-handle walker in execParallel-support
    // exists) nor any per-node `ExecShutdown*` seam is available yet; route the
    // whole walker body through those owner seams — loud panic until they land.
    let _ = (&node, &estate);
    panic!(
        "ExecShutdownNode_walker: route the typed planstate_tree_walker recursion and the \
         per-node ExecShutdown* dispatch (T_GatherState/T_ForeignScanState/T_CustomScanState/\
         T_GatherMergeState/T_HashState/T_HashJoinState) through their owner seams \
         (typed planstate_tree_walker + ExecShutdown* seams not yet available)"
    )
}
