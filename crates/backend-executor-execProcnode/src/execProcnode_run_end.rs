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
/// C `ExecProcNode` is the `executor.h` inline:
///
/// ```c
/// ExecProcNode(PlanState *node)
/// {
///     if (node->chgParam != NULL) /* something changed? */
///         ExecReScan(node);       /* let ReScan handle this */
///     return node->ExecProcNode(node);
/// }
/// ```
///
/// The `chgParam`-triggered `ExecReScan` is part of `ExecProcNode` itself, NOT
/// the per-node callback: it is what implements the executor contract that a
/// parent which leaves a child's `chgParam` set (rather than rescanning it
/// directly) relies on — "the child will be re-scanned by the first
/// `ExecProcNode`". Nodes like Material/Memoize/Agg/SetOp explicitly defer the
/// child rescan to this site (see `ExecReScanMemoize`/`ExecReScanMaterial`). The
/// per-node `ExecProcNode` callback is then armed with [`exec_proc_node_first`]
/// at init time (`ExecSetExecProcNode`), so the first call routes through the
/// first-execution wrapper below.
pub fn exec_proc_node<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let _trace = pgrust_trace::trace_scope!(
        pgrust_trace::Category::Exec,
        "ExecProcNode {}",
        node.tag()
    );

    // if (node->chgParam != NULL) ExecReScan(node);
    if node.ps_head().chgParam.is_some() {
        execAmi::exec_re_scan::call(node, estate)?;
    }

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
    let cb = node.ps_head().ExecProcNode.unwrap_or_else(|| {
        panic!(
            "ExecProcNodeFirst: node ExecProcNode callback missing after first-call rearm (tag={:?})",
            node.tag()
        )
    });
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
    // TupIsNull tests the slot's emptiness, not the pointer: a node at EOF
    // returns its (non-NULL) result slot marked empty, which counts as 0 tuples.
    {
        let is_null = match result {
            None => true,
            Some(id) => estate.slot(id).is_empty(),
        };
        let n_tuples = if is_null { 0.0 } else { 1.0 };
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
///
/// C returns the bare `Node *` and the caller does `IsA(result, TIDBitmap)`;
/// the lone landed multiexec consumer (`nodeBitmapHeapscan`) always demands a
/// `TIDBitmap`, so this unit's owned seam fixes the return at
/// [`TIDBitmap`](types_tidbitmap::TIDBitmap), folding that caller-side `IsA`
/// guard into the seam type. No reachable arm produces a value today (every
/// `MultiExec*` owner is unported), so the narrowing is behaviourally inert.
pub fn multi_exec_proc_node<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, types_tidbitmap::TIDBitmap>> {
    // check_stack_depth();
    tcop_postgres::check_stack_depth::call()?;

    // CHECK_FOR_INTERRUPTS();
    tcop_postgres::check_for_interrupts::call()?;

    // if (node->chgParam != NULL) ExecReScan(node);
    if node.ps_head().chgParam.is_some() {
        execAmi::exec_re_scan::call(node, estate)?;
    }

    // switch (nodeTag(node)) — only node types that actually support multiexec
    // are listed. Each arm runs the owning node unit's `MultiExec*` routine
    // directly (the bitmap owners depend only on this unit's seam crate, so the
    // call is acyclic).
    //
    // case T_BitmapAndState: result = MultiExecBitmapAnd((BitmapAndState *) node);
    //
    // `MultiExecBitmapAnd` takes the whole `&mut PlanStateNode` (it re-derives
    // the concrete `BitmapAndState` internally), so dispatch it before the
    // borrowing `match` below.
    if node.tag() == types_nodes::execstate_tags::T_BitmapAndState {
        return backend_executor_nodeBitmapAnd::MultiExecBitmapAnd(node, estate);
    }

    let mcx = estate.es_query_cxt;
    match node {
        // case T_HashState: result = MultiExecHash((HashState *) node);
        //
        // `MultiExecHash` returns a hashtable `Node`, not a `TIDBitmap`; it does
        // not fit this dispatch's `PgBox<TIDBitmap>` owned-seam return (the lone
        // landed consumer, nodeBitmapHeapscan, always demands a bitmap). Route to
        // the nodeHash owner once a `Node`-returning MultiExec seam exists.
        PlanStateNode::Hash(_) => {
            panic!(
                "MultiExecProcNode(T_HashState): MultiExecHash returns a hashtable Node, \
                 not a TIDBitmap — needs a Node-returning MultiExecProcNode seam"
            )
        }
        // case T_BitmapIndexScanState:
        //     result = MultiExecBitmapIndexScan((BitmapIndexScanState *) node);
        PlanStateNode::BitmapIndexScan(state) => {
            backend_executor_nodeBitmapIndexscan::MultiExecBitmapIndexScan(state, estate)
        }
        // case T_BitmapOrState:  result = MultiExecBitmapOr((BitmapOrState *) node);
        PlanStateNode::BitmapOr(state) => {
            backend_executor_nodeBitmapOr::MultiExecBitmapOr(mcx, state, estate)
        }
        // default: elog(ERROR, "unrecognized node type: %d", (int) nodeTag(node));
        other => Err(PgError::error(format!(
            "unrecognized node type: {}",
            other.tag().0 as i32
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

    // case T_BitmapAndState: ExecEndBitmapAnd((BitmapAndState *) node);
    //
    // `ExecEndBitmapAnd` takes the whole `&mut PlanStateNode` (it re-derives the
    // concrete `BitmapAndState` internally), so dispatch it before the borrowing
    // `match node` below.
    if node.tag() == types_nodes::execstate_tags::T_BitmapAndState {
        return backend_executor_nodeBitmapAnd::ExecEndBitmapAnd(node, estate);
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
        PlanStateNode::Append(append_state) => {
            backend_executor_nodeAppend::ExecEndAppend(append_state, estate)
        }
        // case T_MaterialState: ExecEndMaterial((MaterialState *) node);
        PlanStateNode::Material(material_state) => {
            backend_executor_nodeMaterial::ExecEndMaterial(material_state, estate)
        }
        // case T_MergeAppendState: ExecEndMergeAppend((MergeAppendState *) node);
        PlanStateNode::MergeAppend(state) => {
            backend_executor_nodeMergeAppend::ExecEndMergeAppend(state, estate)
        }
        // case T_MergeJoinState: ExecEndMergeJoin((MergeJoinState *) node);
        PlanStateNode::MergeJoin(state) => {
            backend_executor_nodeMergejoin::ExecEndMergeJoin(state, estate)
        }
        // case T_MemoizeState: ExecEndMemoize((MemoizeState *) node);
        PlanStateNode::Memoize(state) => {
            backend_executor_nodeMemoize::ExecEndMemoize(state, estate)
        }
        // case T_IndexOnlyScanState: ExecEndIndexOnlyScan((IndexOnlyScanState *) node);
        PlanStateNode::IndexOnlyScan(state) => {
            backend_executor_nodeIndexonlyscan::ExecEndIndexOnlyScan(state, estate)
        }
        // case T_LimitState: ExecEndLimit((LimitState *) node);
        PlanStateNode::Limit(limit_state) => {
            backend_executor_nodeLimit::ExecEndLimit(limit_state, estate)
        }
        // case T_LockRowsState: ExecEndLockRows((LockRowsState *) node);
        PlanStateNode::LockRows(state) => {
            backend_executor_nodeLockRows::ExecEndLockRows(state, estate)
        }
        // case T_SortState: ExecEndSort((SortState *) node);
        PlanStateNode::Sort(state) => backend_executor_nodeSort::ExecEndSort(state, estate),
        // case T_IncrementalSortState:
        //   ExecEndIncrementalSort((IncrementalSortState *) node);
        PlanStateNode::IncrementalSort(state) => {
            backend_executor_nodeIncrementalSort::ExecEndIncrementalSort(state, estate)
        }
        // case T_TableFuncScanState: ExecEndTableFuncScan((TableFuncScanState *) node);
        //
        // `ExecEndTableFuncScan` releases only the node's own tuplestore and
        // takes no `EState` (the C routine ignores its estate); call it with the
        // state struct alone.
        PlanStateNode::TableFuncScan(state) => {
            backend_executor_nodeTableFuncscan::ExecEndTableFuncScan(state)
        }
        // case T_NestLoopState: ExecEndNestLoop((NestLoopState *) node);
        PlanStateNode::NestLoop(state) => {
            backend_executor_nodeNestloop::ExecEndNestLoop(state, estate)
        }
        // case T_HashJoinState: ExecEndHashJoin((HashJoinState *) node);
        PlanStateNode::HashJoin(state) => {
            backend_executor_nodeHashjoin::ExecEndHashJoin(state, estate)
        }
        // case T_SeqScanState: ExecEndSeqScan((SeqScanState *) node);
        //
        // `ExecEndSeqScan` closes the table-AM scan via its own seams and takes
        // no `EState`; call it with the state struct alone.
        PlanStateNode::SeqScan(state) => backend_executor_nodeSeqscan::ExecEndSeqScan(state),
        // case T_ForeignScanState: ExecEndForeignScan((ForeignScanState *) node);
        PlanStateNode::ForeignScan(state) => {
            backend_executor_nodeForeignscan::ExecEndForeignScan(state, estate)
        }
        // case T_GatherState: ExecEndGather((GatherState *) node);
        PlanStateNode::Gather(state) => {
            backend_executor_nodeGather::ExecEndGather(state, estate)
        }
        // case T_HashState: ExecEndHash((HashState *) node);
        PlanStateNode::Hash(state) => {
            backend_executor_nodeHash::exec_hash::ExecEndHash(state, estate)
        }
        // case T_ResultState: ExecEndResult((ResultState *) node);
        PlanStateNode::Result(state) => {
            backend_executor_nodeResult::ExecEndResult(state, estate)
        }
        // case T_ProjectSetState: ExecEndProjectSet((ProjectSetState *) node);
        PlanStateNode::ProjectSet(state) => {
            backend_executor_nodeProjectSet::ExecEndProjectSet(state, estate)
        }
        // case T_ModifyTableState: ExecEndModifyTable((ModifyTableState *) node);
        PlanStateNode::ModifyTable(state) => {
            backend_executor_nodeModifyTable::lifecycle::ExecEndModifyTable(state, estate)
        }
        // case T_RecursiveUnionState: ExecEndRecursiveUnion((RecursiveUnionState *) node);
        PlanStateNode::RecursiveUnion(state) => {
            backend_executor_nodeRecursiveunion::ExecEndRecursiveUnion(state, estate)
        }
        // case T_BitmapOrState: ExecEndBitmapOr((BitmapOrState *) node);
        PlanStateNode::BitmapOr(state) => {
            backend_executor_nodeBitmapOr::ExecEndBitmapOr(state, estate)
        }
        // case T_GatherMergeState: ExecEndGatherMerge((GatherMergeState *) node);
        PlanStateNode::GatherMerge(state) => {
            backend_executor_nodeGatherMerge::ExecEndGatherMerge(state, estate)
        }
        // case T_IndexScanState: ExecEndIndexScan((IndexScanState *) node);
        PlanStateNode::IndexScan(state) => {
            backend_executor_nodeIndexscan::ExecEndIndexScan(state, estate)
        }
        // case T_BitmapIndexScanState: ExecEndBitmapIndexScan((BitmapIndexScanState *) node);
        PlanStateNode::BitmapIndexScan(state) => {
            backend_executor_nodeBitmapIndexscan::ExecEndBitmapIndexScan(state, estate)
        }
        // case T_BitmapHeapScanState: ExecEndBitmapHeapScan((BitmapHeapScanState *) node);
        PlanStateNode::BitmapHeapScan(state) => {
            backend_executor_nodeBitmapHeapscan::ExecEndBitmapHeapScan(state, estate)
        }
        // case T_TidScanState: ExecEndTidScan((TidScanState *) node);
        //
        // `ExecEndTidScan` closes its own table-AM scan and takes no `EState`.
        PlanStateNode::TidScan(state) => {
            backend_executor_nodeTidscan::ExecEndTidScan(state)
        }
        // case T_SampleScanState: ExecEndSampleScan((SampleScanState *) node);
        //
        // `SampleScanState` lives in `types-samplescan` (ABOVE `types-nodes`), so
        // the carrier is downcast to the concrete state (tag-checked) before the
        // node crate's `ExecEndSampleScan` runs (which closes its own table-AM
        // scan via seams and takes no `EState`).
        PlanStateNode::SampleScan(s) => {
            let sample = types_nodes::samplescanstate_carrier::downcast_sample_scan_state_mut::<
                types_samplescan::SampleScanState<'_>,
            >(&mut **s)
            .expect("castNode(SampleScanState, node) failed");
            backend_executor_nodeSamplescan::ExecEndSampleScan(sample)
        }
        // case T_TidRangeScanState: ExecEndTidRangeScan((TidRangeScanState *) node);
        PlanStateNode::TidRangeScan(state) => {
            backend_executor_nodeTidrangescan::ExecEndTidRangeScan(state, estate)
        }
        // case T_SubqueryScanState: ExecEndSubqueryScan((SubqueryScanState *) node);
        PlanStateNode::SubqueryScan(state) => {
            backend_executor_nodeSubqueryscan::ExecEndSubqueryScan(state, estate)
        }
        // case T_CteScanState: ExecEndCteScan((CteScanState *) node);
        //
        // `ExecEndCteScan` frees the shared tuplestore only when this node is its
        // own leader (`node->leader == node`); in the owned model the leader
        // identity was recorded on the node by `cte_resolve_leader`, surfaced
        // through the `cte_leader_is_self` seam. Freeing clears the shared store
        // held in `EState.es_cte_shared[cteParam]`, hence the `&mut estate`.
        PlanStateNode::CteScan(state) => {
            let is_leader =
                backend_executor_execMain_seams::cte_leader_is_self::call(state)?;
            backend_executor_nodeCtescan::ExecEndCteScan(state, is_leader, estate)
        }
        // case T_CustomScanState: ExecEndCustomScan((CustomScanState *) node);
        PlanStateNode::CustomScan(state) => {
            backend_executor_nodeCustom::ExecEndCustomScan(state, estate)
        }
        // case T_GroupState: ExecEndGroup((GroupState *) node);
        PlanStateNode::Group(state) => {
            backend_executor_nodeGroup::ExecEndGroup(state, estate)
        }
        // case T_UniqueState: ExecEndUnique((UniqueState *) node);
        PlanStateNode::Unique(state) => {
            backend_executor_nodeUnique::ExecEndUnique(state, estate)
        }
        // case T_SetOpState: ExecEndSetOp((SetOpState *) node);
        PlanStateNode::SetOp(state) => {
            backend_executor_nodeSetOp::ExecEndSetOp(state, estate)
        }
        // case T_AggState: ExecEndAgg((AggState *) node);
        PlanStateNode::Agg(a) => {
            let agg = types_nodes::aggstate_carrier::downcast_agg_state_mut::<
                backend_executor_nodeAgg::AggStateData<'_>,
            >(&mut **a)
            .expect("castNode(AggState, node) failed");
            backend_executor_nodeAgg::ExecEndAgg(agg, estate)
        }

        // case T_WindowAggState: ExecEndWindowAgg((WindowAggState *) node);
        PlanStateNode::WindowAgg(state) => {
            backend_executor_nodeWindowAgg::ExecEndWindowAgg(state, estate)
        }
        // case T_FunctionScanState: ExecEndFunctionScan((FunctionScanState *) node);
        PlanStateNode::FunctionScan(state) => {
            backend_executor_nodeFunctionscan::ExecEndFunctionScan(state)
        }
        // No clean up actions for these nodes:
        //   case T_ValuesScanState:
        //   case T_NamedTuplestoreScanState:
        //   case T_WorkTableScanState:
        //       break;
        PlanStateNode::ValuesScan(_)
        | PlanStateNode::NamedTuplestoreScan(_)
        | PlanStateNode::WorkTableScan(_) => Ok(()),

        // The remaining C arms (T_SampleScanState/
        // T_IncrementalSortState/T_AggState)
        // operate on node-state variants not yet present in
        // the `#[non_exhaustive]` `PlanStateNode` enum, so their tags cannot occur.
        // The C `default: elog(ERROR, "unrecognized node type")` covers any tag
        // with no arm.
        other => Err(PgError::error(format!(
            "unrecognized node type: {}",
            other.tag().0 as i32
        ))),
    }
}

/// `ExecSetTupleBound(tuples_needed, child_node)` (execProcnode.c).
///
/// Inform a node — and, where it is safe to do so, applicable descendants —
/// that no more than `tuples_needed` tuples will be demanded from it. A
/// negative bound means "no limit". Mirrors the C cascade of `IsA(child_node,
/// …)` tests:
///   * `SortState`/`IncrementalSortState`: set/clear `bounded`/`bound`;
///   * `AppendState`/`MergeAppendState`: push the bound to every child input;
///   * projecting `ResultState`: push to its outer child (if any);
///   * `SubqueryScanState` with no qual: push to its subplan;
///   * `GatherState`/`GatherMergeState`: record `tuples_needed` and push to the
///     local copy of the child plan.
/// Any other node type stops propagation (no descent), the C fall-through.
///
/// Only the node-state variants whose executor units have landed
/// (`SortState`/`IncrementalSortState`/`AppendState`/`MergeAppendState`) are
/// present in the `#[non_exhaustive]` `PlanStateNode` enum, so the remaining C
/// `IsA` arms (`ResultState`/`SubqueryScanState`/`GatherState`/
/// `GatherMergeState`) cannot occur yet; they are added here as their units
/// land. Every other tag is the C final fall-through (a no-op).
pub fn exec_set_tuple_bound<'mcx>(
    tuples_needed: i64,
    child_node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Since this function recurses, in principle we should check stack depth
    // here.  In practice, it's probably pointless since the earlier node
    // initialization tree traversal would surely have consumed more stack.
    match child_node {
        // if (IsA(child_node, SortState))
        PlanStateNode::Sort(sort_state) => {
            if tuples_needed < 0 {
                // make sure flag gets reset if needed upon rescan
                sort_state.bounded = false;
            } else {
                sort_state.bounded = true;
                sort_state.bound = tuples_needed;
            }
        }
        // else if (IsA(child_node, IncrementalSortState))
        // If it is an IncrementalSort node, notify it that it can use bounded
        // sort. (It is nodeIncrementalSort.c's responsibility to react properly
        // to changes of these parameters.)
        PlanStateNode::IncrementalSort(sort_state) => {
            if tuples_needed < 0 {
                // make sure flag gets reset if needed upon rescan
                sort_state.bounded = false;
            } else {
                sort_state.bounded = true;
                sort_state.bound = tuples_needed;
            }
        }
        // else if (IsA(child_node, AppendState))
        PlanStateNode::Append(append_state) => {
            // for (i = 0; i < aState->as_nplans; i++)
            //     ExecSetTupleBound(tuples_needed, aState->appendplans[i]);
            let n = append_state.as_nplans as usize;
            for i in 0..n {
                let child = append_state.appendplans[i]
                    .as_mut()
                    .expect("ExecSetTupleBound: AppendState.appendplans slot is NULL");
                exec_set_tuple_bound(tuples_needed, child, estate)?;
            }
        }
        // else if (IsA(child_node, MergeAppendState))
        PlanStateNode::MergeAppend(ma_state) => {
            // for (i = 0; i < maState->ms_nplans; i++)
            //     ExecSetTupleBound(tuples_needed, maState->mergeplans[i]);
            let n = ma_state.ms_nplans as usize;
            for i in 0..n {
                let child = ma_state.mergeplans[i]
                    .as_mut()
                    .expect("ExecSetTupleBound: MergeAppendState.mergeplans slot is NULL");
                exec_set_tuple_bound(tuples_needed, child, estate)?;
            }
        }
        // else if (IsA(child_node, SubqueryScanState))
        // We can also descend through SubqueryScan, but only if it has no qual
        // (otherwise it might discard rows).
        PlanStateNode::SubqueryScan(subquery_state) => {
            if subquery_state.ss.ps.qual.is_none() {
                if let Some(subplan) = subquery_state.subplan.as_mut() {
                    exec_set_tuple_bound(tuples_needed, subplan, estate)?;
                }
            }
        }
        // The remaining C `IsA` arms — the projecting ResultState (descend
        // through outerPlanState), GatherState and GatherMergeState (record
        // tuples_needed + descend through outerPlanState) — operate on
        // node-state variants not yet present in the `#[non_exhaustive]`
        // `PlanStateNode` enum, so their tags cannot occur. Each adds its arm
        // here as its executor unit lands.
        //
        // Otherwise, on seeing a node that can discard or combine input rows we
        // can't propagate the bound any further; do nothing (the C
        // fall-through).
        _ => {}
    }

    Ok(())
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

    // Treat the node as running while we shut it down, but only if it's run at
    // least once already (in the case of Gather/Gather Merge we may shut down
    // workers here, propagating their buffer usage into the node's instrument).
    //   if (node->instrument && node->instrument->running)
    //       InstrStartNode(node->instrument);
    let started = match node.ps_head_mut().instrument.as_deref_mut() {
        Some(instr) if instr.running => {
            instrument::instr_start_node::call(instr)?;
            true
        }
        _ => false,
    };

    // planstate_tree_walker(node, ExecShutdownNode_walker, context);
    //
    // Recurse over the child PlanState nodes. Mirrors the typed
    // `planstate_tree_walker` (the same `planstate_tree_walker_children_mut`
    // child enumeration it drives), recursing directly to avoid the closure
    // lifetime invariance of threading `&mut EStateData<'mcx>` through the
    // walker callback (same shape as execParallel's owned tree walks). The C
    // return is always `false`; a nested ereport(ERROR) is propagated.
    for child in node.planstate_tree_walker_children_mut() {
        exec_shutdown_node_walker(child, estate)?;
    }

    // switch (nodeTag(node)) — per-node shutdown dispatch.
    match node {
        // case T_GatherState: ExecShutdownGather((GatherState *) node);
        PlanStateNode::Gather(state) => {
            backend_executor_nodeGather::ExecShutdownGather(state, estate)?;
        }
        // case T_ForeignScanState: ExecShutdownForeignScan((ForeignScanState *) node);
        PlanStateNode::ForeignScan(state) => {
            backend_executor_nodeForeignscan::ExecShutdownForeignScan(state, estate)?;
        }
        // case T_CustomScanState: ExecShutdownCustomScan((CustomScanState *) node);
        PlanStateNode::CustomScan(state) => {
            backend_executor_nodeCustom::ExecShutdownCustomScan(state, estate)?;
        }
        // case T_GatherMergeState: ExecShutdownGatherMerge((GatherMergeState *) node);
        PlanStateNode::GatherMerge(state) => {
            backend_executor_nodeGatherMerge::ExecShutdownGatherMerge(state, estate)?;
        }
        // case T_HashState: ExecShutdownHash((HashState *) node);
        PlanStateNode::Hash(state) => {
            let mcx = estate.es_query_cxt;
            backend_executor_nodeHash::exec_hash::ExecShutdownHash(mcx, state)?;
        }
        // case T_HashJoinState: ExecShutdownHashJoin((HashJoinState *) node);
        PlanStateNode::HashJoin(state) => {
            backend_executor_nodeHashjoin::ExecShutdownHashJoin(state)?;
        }
        // default: break;
        _ => {}
    }

    // Stop the node if we started it above, reporting 0 tuples.
    //   if (node->instrument && node->instrument->running)
    //       InstrStopNode(node->instrument, 0);
    if started {
        let instr = node
            .ps_head_mut()
            .instrument
            .as_deref_mut()
            .expect("ExecShutdownNode_walker: instrument vanished between start and stop");
        instrument::instr_stop_node::call(instr, 0.0)?;
    }

    // return false;
    Ok(false)
}
