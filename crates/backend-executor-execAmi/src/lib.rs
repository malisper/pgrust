//! Port of `src/backend/executor/execAmi.c` — miscellaneous executor
//! access-method routines.
//!
//! Entry points (1:1 with the C):
//! - [`exec_re_scan`] (`ExecReScan`)
//! - [`exec_mark_pos`] (`ExecMarkPos`)
//! - [`exec_restr_pos`] (`ExecRestrPos`)
//! - [`exec_supports_mark_restore`] (`ExecSupportsMarkRestore`)
//! - [`exec_supports_backward_scan`] (`ExecSupportsBackwardScan`)
//! - `index_supports_backward_scan` (the file-static, private as in the C)
//! - [`exec_materializes_output`] (`ExecMaterializesOutput`)
//!
//! The per-node-type dispatches (`switch (nodeTag(node))`) live HERE, matching
//! on the owned dispatch enums ([`PlanStateNode`], [`Node`], [`PathNode`]).
//! Arms whose node type has no enum variant yet are covered by the wildcard
//! (the C `default:`); each node port that adds a variant must add its arm
//! here. Calls into unported owners (instrument.c, nodeSubplan.c,
//! syscache.c, amapi.c) go through those owners' seam crates and panic until
//! the owners land; execUtils.c is ported and a direct dependency.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use backend_access_index_amapi_seams as amapi;
use backend_executor_execUtils as execUtils;
use backend_executor_instrument_seams as instrument;
use backend_executor_nodeLimit as nodeLimit;
use backend_executor_nodeMaterial as nodeMaterial;
use backend_executor_nodeSubplan_seams as nodeSubplan;
use backend_executor_nodeAppend as nodeAppend;
use backend_executor_nodeBitmapHeapscan as nodeBitmapHeapscan;
use backend_executor_nodeBitmapIndexscan as nodeBitmapIndexscan;
use backend_executor_nodeBitmapOr as nodeBitmapOr;
use backend_executor_nodeCtescan as nodeCtescan;
use backend_executor_nodeCustom as nodeCustom;
use backend_executor_nodeForeignscan as nodeForeignscan;
use backend_executor_nodeGather as nodeGather;
use backend_executor_nodeGatherMerge as nodeGatherMerge;
use backend_executor_nodeGroup as nodeGroup;
use backend_executor_nodeAgg as nodeAgg;
use backend_executor_nodeHash as nodeHash;
use backend_executor_nodeHashjoin as nodeHashjoin;
use backend_executor_nodeIndexonlyscan as nodeIndexonlyscan;
use backend_executor_nodeIndexscan as nodeIndexscan;
use backend_executor_nodeMemoize as nodeMemoize;
use backend_executor_nodeMergeAppend as nodeMergeAppend;
use backend_executor_nodeMergejoin as nodeMergejoin;
use backend_executor_nodeModifyTable as nodeModifyTable;
use backend_executor_nodeNamedtuplestorescan as nodeNamedtuplestorescan;
use backend_executor_nodeNestloop as nodeNestloop;
use backend_executor_nodeProjectSet as nodeProjectSet;
use backend_executor_nodeRecursiveunion as nodeRecursiveunion;
use backend_executor_nodeResult as nodeResult;
use backend_executor_nodeSeqscan as nodeSeqscan;
use backend_executor_nodeSetOp as nodeSetOp;
use backend_executor_nodeSort as nodeSort;
use backend_executor_nodeIncrementalSort as nodeIncrementalSort;
use backend_executor_nodeSubqueryscan as nodeSubqueryscan;
use backend_executor_nodeTableFuncscan as nodeTableFuncscan;
use backend_executor_nodeFunctionscan as nodeFunctionscan;
use backend_executor_nodeTidscan as nodeTidscan;
use backend_executor_nodeTidrangescan as nodeTidrangescan;
use backend_executor_nodeWorktablescan as nodeWorktablescan;
use backend_executor_nodeUnique as nodeUnique;
use backend_executor_nodeValuesscan as nodeValuesscan;
use backend_executor_nodeWindowAgg as nodeWindowAgg;
use backend_utils_cache_syscache_seams as syscache;
use backend_utils_error::elog;
use types_core::Oid;
use types_nodes::nodes::NodeTag;
use types_error::{PgError, PgResult, DEBUG2};
use types_nodes::nodeindexscan::CUSTOMPATH_SUPPORT_MARK_RESTORE;
use types_nodes::nodes::{
    ntag, Node, T_CteScan, T_FunctionScan, T_IndexOnlyScan, T_IndexScan, T_Material, T_MergeAppend,
    T_NamedTuplestoreScan, T_Result, T_Sort, T_TableFuncScan, T_WorkTableScan, T_Append,
    T_CustomScan,
};
use types_nodes::pathnodes::PathNode;
use types_nodes::{EStateData, PlanStateNode};

/// Install this crate's implementations into its seam slots.
pub fn init_seams() {
    backend_executor_execAmi_seams::exec_re_scan::set(exec_re_scan);
    backend_executor_execAmi_seams::exec_mark_pos::set(exec_mark_pos);
    backend_executor_execAmi_seams::exec_restr_pos::set(exec_restr_pos);

    // `ExecMaterializesOutput(nodeTag(plan))` (execAmi.c) — pure node-tag
    // classification consumed by `build_subplan` in init-subselect to decide
    // whether to add a `Material`. The owner body lives here.
    backend_optimizer_plan_init_subselect_ext_seams::exec_materializes_output::set(
        exec_materializes_output,
    );
    // The same classifier is also read by joinpath.c (`try_nestloop_path` /
    // `match_unsorted_outer`, deciding whether the inner path already
    // materializes) through joinpath-seams; install the owner body there too.
    backend_optimizer_path_joinpath_seams::exec_materializes_output::set(
        exec_materializes_output,
    );
}

/// `elog(ERROR, "unrecognized node type: %d", (int) nodeTag(node))` — carries
/// `ERRCODE_INTERNAL_ERROR`, as every bare `elog(ERROR)` does.
fn unrecognized_node_type(tag: NodeTag) -> PgError {
    PgError::error(format!("unrecognized node type: {tag}"))
}

/// `ExecReScan(node)` — reset a plan node so that its output can be
/// re-scanned.
///
/// Note that if the plan node has parameters that have changed value, the
/// output might be different from last time.
///
/// `estate` is the owned-model threading of the C `node->state` back-pointer;
/// its `es_query_cxt` is the C `CurrentMemoryContext` the param-set updates
/// allocate in.
pub fn exec_re_scan<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // If collecting timing stats, update them.
    if let Some(instr) = node.ps_head_mut().instrument.as_deref_mut() {
        instrument::instr_end_loop::call(instr)?;
    }

    // If we have changed parameters, propagate that info.
    //
    // Note: ExecReScanSetParamPlan() can add bits to node->chgParam,
    // corresponding to the output param(s) that the InitPlan will update.
    // Since we make only one pass over the list, that means that an InitPlan
    // can depend on the output param(s) of a sibling InitPlan only if that
    // sibling appears earlier in the list.  This is workable for now given
    // the limited ways in which one InitPlan could depend on another, but
    // eventually we might need to work harder (or else make the planner
    // enlarge the extParam/allParam sets to include the params of depended-on
    // InitPlans).
    if node.ps_head().chgParam.is_some() {
        let mcx = estate.es_query_cxt;
        let head = node.ps_head_mut();

        // foreach(l, node->initPlan)
        let init_len = head.initPlan.as_ref().map_or(0, |l| l.len());
        for i in 0..init_len {
            // SubPlanState *sstate = lfirst(l); PlanState *splan = sstate->planstate;
            //
            // if (splan->plan->extParam != NULL)  /* don't care about child
            //                                      * local Params */
            //     UpdateChangedParamSet(splan, node->chgParam);
            let splan_has_ext_param = head.initPlan.as_ref().expect("checked above")[i]
                .planstate
                .as_deref()
                .and_then(|splan| splan.ps_head().plan)
                .is_some_and(|plan| plan.plan_head().extParam.is_some());
            if splan_has_ext_param {
                let newchg = head
                    .chgParam
                    .as_deref()
                    .expect("ExecReScan: chgParam went NULL during the InitPlan walk");
                let splan = head.initPlan.as_mut().expect("checked above")[i]
                    .planstate
                    .as_deref_mut()
                    .expect("ExecReScan: initPlan planstate is NULL");
                execUtils::UpdateChangedParamSet(splan.ps_head_mut(), Some(newchg), mcx)?;
            }

            // if (splan->chgParam != NULL)
            //     ExecReScanSetParamPlan(sstate, node);
            //
            // (re-tested AFTER the update above: UpdateChangedParamSet may
            // have set splan->chgParam.)
            let splan_has_chg_param = head.initPlan.as_ref().expect("checked above")[i]
                .planstate
                .as_deref()
                .is_some_and(|splan| splan.ps_head().chgParam.is_some());
            if splan_has_chg_param {
                let sstate = &mut head.initPlan.as_mut().expect("checked above")[i];
                nodeSubplan::exec_re_scan_set_param_plan::call(
                    sstate,
                    &mut head.chgParam,
                    estate,
                )?;
            }
        }

        // foreach(l, node->subPlan)
        let sub_len = head.subPlan.as_ref().map_or(0, |l| l.len());
        for i in 0..sub_len {
            // if (splan->plan->extParam != NULL)
            //     UpdateChangedParamSet(splan, node->chgParam);
            let splan_has_ext_param = head.subPlan.as_ref().expect("checked above")[i]
                .planstate
                .as_deref()
                .and_then(|splan| splan.ps_head().plan)
                .is_some_and(|plan| plan.plan_head().extParam.is_some());
            if splan_has_ext_param {
                let newchg = head
                    .chgParam
                    .as_deref()
                    .expect("ExecReScan: chgParam went NULL during the subPlan walk");
                let splan = head.subPlan.as_mut().expect("checked above")[i]
                    .planstate
                    .as_deref_mut()
                    .expect("ExecReScan: subPlan planstate is NULL");
                execUtils::UpdateChangedParamSet(splan.ps_head_mut(), Some(newchg), mcx)?;
            }
        }

        // Well. Now set chgParam for child trees.
        if head.lefttree.is_some() {
            let newchg = head
                .chgParam
                .as_deref()
                .expect("ExecReScan: chgParam went NULL before the child-tree walk");
            let outer = head.lefttree.as_deref_mut().expect("checked above");
            execUtils::UpdateChangedParamSet(outer.ps_head_mut(), Some(newchg), mcx)?;
        }
        if head.righttree.is_some() {
            let newchg = head
                .chgParam
                .as_deref()
                .expect("ExecReScan: chgParam went NULL before the child-tree walk");
            let inner = head.righttree.as_deref_mut().expect("checked above");
            execUtils::UpdateChangedParamSet(inner.ps_head_mut(), Some(newchg), mcx)?;
        }
    }

    // Call expression callbacks.
    if let Some(ecxt_id) = node.ps_head_mut().ps_ExprContext {
        execUtils::ReScanExprContext(estate.ecxt_mut(ecxt_id))?;
    }

    // And do node-type-specific processing.
    //
    // case T_BitmapAndState: ExecReScanBitmapAnd((BitmapAndState *) node);
    //
    // `ExecReScanBitmapAnd` takes the whole `&mut PlanStateNode` (it re-derives
    // the concrete `BitmapAndState` internally), so dispatch it before the
    // borrowing `match` below; the C `chgParam` free at the tail still applies.
    let mcx = estate.es_query_cxt;
    if node.tag() == types_nodes::execstate_tags::T_BitmapAndState {
        backend_executor_nodeBitmapAnd::ExecReScanBitmapAnd(node, estate)?;
    } else {
        match node {
            // case T_ResultState: ExecReScanResult((ResultState *) node);
            PlanStateNode::Result(m) => nodeResult::ExecReScanResult(m, estate)?,
            // case T_ProjectSetState: ExecReScanProjectSet((ProjectSetState *) node);
            PlanStateNode::ProjectSet(m) => nodeProjectSet::ExecReScanProjectSet(m, estate)?,
            // case T_ModifyTableState: ExecReScanModifyTable((ModifyTableState *) node);
            PlanStateNode::ModifyTable(m) => {
                nodeModifyTable::lifecycle::ExecReScanModifyTable(m)?
            }
            // case T_AppendState: ExecReScanAppend((AppendState *) node);
            PlanStateNode::Append(m) => nodeAppend::ExecReScanAppend(mcx, m, estate)?,
            // case T_MergeAppendState: ExecReScanMergeAppend((MergeAppendState *) node);
            PlanStateNode::MergeAppend(m) => nodeMergeAppend::ExecReScanMergeAppend(m, estate)?,
            // case T_RecursiveUnionState: ExecReScanRecursiveUnion((RecursiveUnionState *) node);
            PlanStateNode::RecursiveUnion(m) => {
                nodeRecursiveunion::ExecReScanRecursiveUnion(m, estate)?
            }
            // case T_BitmapOrState: ExecReScanBitmapOr((BitmapOrState *) node);
            PlanStateNode::BitmapOr(m) => nodeBitmapOr::ExecReScanBitmapOr(mcx, m, estate)?,
            // case T_SeqScanState: ExecReScanSeqScan((SeqScanState *) node);
            PlanStateNode::SeqScan(m) => nodeSeqscan::ExecReScanSeqScan(m, estate)?,
            // case T_GatherState: ExecReScanGather((GatherState *) node);
            PlanStateNode::Gather(m) => nodeGather::ExecReScanGather(m, estate)?,
            // case T_GatherMergeState: ExecReScanGatherMerge((GatherMergeState *) node);
            PlanStateNode::GatherMerge(m) => nodeGatherMerge::ExecReScanGatherMerge(m, estate)?,
            // case T_IndexScanState: ExecReScanIndexScan((IndexScanState *) node);
            PlanStateNode::IndexScan(m) => nodeIndexscan::ExecReScanIndexScan(m, estate)?,
            // case T_IndexOnlyScanState: ExecReScanIndexOnlyScan((IndexOnlyScanState *) node);
            PlanStateNode::IndexOnlyScan(m) => {
                nodeIndexonlyscan::ExecReScanIndexOnlyScan(m, estate)?
            }
            // case T_BitmapIndexScanState: ExecReScanBitmapIndexScan((BitmapIndexScanState *) node);
            PlanStateNode::BitmapIndexScan(m) => {
                nodeBitmapIndexscan::ExecReScanBitmapIndexScan(m, estate)?
            }
            // case T_BitmapHeapScanState: ExecReScanBitmapHeapScan((BitmapHeapScanState *) node);
            PlanStateNode::BitmapHeapScan(m) => {
                nodeBitmapHeapscan::ExecReScanBitmapHeapScan(m, estate)?
            }
            // case T_TidScanState: ExecReScanTidScan((TidScanState *) node);
            PlanStateNode::TidScan(m) => nodeTidscan::ExecReScanTidScan(m, estate)?,
            // case T_WorkTableScanState: ExecReScanWorkTableScan((WorkTableScanState *) node);
            PlanStateNode::WorkTableScan(m) => {
                nodeWorktablescan::ExecReScanWorkTableScan(m, estate)?
            }
            // case T_SubqueryScanState: ExecReScanSubqueryScan((SubqueryScanState *) node);
            PlanStateNode::SubqueryScan(m) => {
                nodeSubqueryscan::ExecReScanSubqueryScan(m, estate)?
            }
            // case T_TableFuncScanState: ExecReScanTableFuncScan((TableFuncScanState *) node);
            PlanStateNode::TableFuncScan(m) => {
                nodeTableFuncscan::ExecReScanTableFuncScan(m, estate)?
            }
            // case T_FunctionScanState: ExecReScanFunctionScan((FunctionScanState *) node);
            PlanStateNode::FunctionScan(m) => {
                nodeFunctionscan::ExecReScanFunctionScan(m, estate)?
            }
            // case T_ValuesScanState: ExecReScanValuesScan((ValuesScanState *) node);
            PlanStateNode::ValuesScan(m) => nodeValuesscan::ExecReScanValuesScan(m, estate)?,
            // case T_CteScanState: ExecReScanCteScan((CteScanState *) node);
            PlanStateNode::CteScan(m) => nodeCtescan::ExecReScanCteScan(m, estate)?,
            // case T_NamedTuplestoreScanState:
            //     ExecReScanNamedTuplestoreScan((NamedTuplestoreScanState *) node);
            PlanStateNode::NamedTuplestoreScan(m) => {
                nodeNamedtuplestorescan::ExecReScanNamedTuplestoreScan(m, estate)?
            }
            // case T_ForeignScanState: ExecReScanForeignScan((ForeignScanState *) node);
            PlanStateNode::ForeignScan(m) => nodeForeignscan::ExecReScanForeignScan(m, estate)?,
            // case T_CustomScanState: ExecReScanCustomScan((CustomScanState *) node);
            PlanStateNode::CustomScan(m) => nodeCustom::ExecReScanCustomScan(m, estate)?,
            // case T_NestLoopState: ExecReScanNestLoop((NestLoopState *) node);
            PlanStateNode::NestLoop(m) => nodeNestloop::ExecReScanNestLoop(m, estate)?,
            // case T_MergeJoinState: ExecReScanMergeJoin((MergeJoinState *) node);
            PlanStateNode::MergeJoin(m) => nodeMergejoin::ExecReScanMergeJoin(m, estate)?,
            // case T_HashJoinState: ExecReScanHashJoin((HashJoinState *) node);
            PlanStateNode::HashJoin(m) => nodeHashjoin::ExecReScanHashJoin(m, estate)?,
            // case T_MaterialState: ExecReScanMaterial((MaterialState *) node);
            PlanStateNode::Material(m) => nodeMaterial::ExecReScanMaterial(m, estate)?,
            // case T_MemoizeState: ExecReScanMemoize((MemoizeState *) node);
            PlanStateNode::Memoize(m) => nodeMemoize::ExecReScanMemoize(m, estate)?,
            // case T_SortState: ExecReScanSort((SortState *) node);
            PlanStateNode::Sort(m) => nodeSort::ExecReScanSort(m, estate)?,
            // case T_IncrementalSortState:
            //   ExecReScanIncrementalSort((IncrementalSortState *) node);
            PlanStateNode::IncrementalSort(m) => {
                nodeIncrementalSort::ExecReScanIncrementalSort(m, estate)?
            }
            // case T_GroupState: ExecReScanGroup((GroupState *) node);
            PlanStateNode::Group(m) => nodeGroup::ExecReScanGroup(m, estate)?,
            // case T_AggState: ExecReScanAgg((AggState *) node);
            PlanStateNode::Agg(a) => {
                let agg = types_nodes::aggstate_carrier::downcast_agg_state_mut::<
                    nodeAgg::AggStateData<'_>,
                >(&mut **a)
                .expect("castNode(AggState, node) failed");
                nodeAgg::ExecReScanAgg(agg, estate)?
            }
            // case T_WindowAggState: ExecReScanWindowAgg((WindowAggState *) node);
            PlanStateNode::WindowAgg(m) => nodeWindowAgg::ExecReScanWindowAgg(m, estate)?,
            // case T_UniqueState: ExecReScanUnique((UniqueState *) node);
            PlanStateNode::Unique(m) => nodeUnique::ExecReScanUnique(m, estate)?,
            // case T_HashState: ExecReScanHash((HashState *) node);
            PlanStateNode::Hash(m) => nodeHash::exec_hash::ExecReScanHash(m, estate)?,
            // case T_SetOpState: ExecReScanSetOp((SetOpState *) node);
            PlanStateNode::SetOp(m) => nodeSetOp::ExecReScanSetOp(m, estate)?,
            // case T_LimitState: ExecReScanLimit((LimitState *) node);
            PlanStateNode::Limit(m) => nodeLimit::ExecReScanLimit(m, estate)?,

            // case T_TidRangeScanState: ExecReScanTidRangeScan((TidRangeScanState *) node);
            PlanStateNode::TidRangeScan(m) => {
                nodeTidrangescan::ExecReScanTidRangeScan(m, estate)?
            }

            // The remaining C arms (T_SampleScanState/
            // T_AggState/T_LockRowsState) operate on node-state variants not yet
            // present in PlanStateNode, so their tags cannot occur. C default:
            //   elog(ERROR, "unrecognized node type: %d", (int) nodeTag(node));
            other => return Err(unrecognized_node_type(other.tag())),
        }
    }

    // if (node->chgParam != NULL) { bms_free(node->chgParam); node->chgParam = NULL; }
    node.ps_head_mut().chgParam = None;

    Ok(())
}

/// `ExecMarkPos(node)` — marks the current scan position.
///
/// NOTE: mark/restore capability is currently needed only for plan nodes
/// that are the immediate inner child of a MergeJoin node.  Since MergeJoin
/// requires sorted input, there is never any need to support mark/restore in
/// node types that cannot produce sorted output.  There are some cases in
/// which a node can pass through sorted data from its child; if we don't
/// implement mark/restore for such a node type, the planner compensates by
/// inserting a Material node above that node.
pub fn exec_mark_pos<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    match node {
        // case T_IndexScanState: ExecIndexMarkPos((IndexScanState *) node);
        PlanStateNode::IndexScan(m) => nodeIndexscan::ExecIndexMarkPos(m, estate),
        // case T_IndexOnlyScanState: ExecIndexOnlyMarkPos((IndexOnlyScanState *) node);
        PlanStateNode::IndexOnlyScan(m) => nodeIndexonlyscan::ExecIndexOnlyMarkPos(m, estate),
        // case T_CustomScanState: ExecCustomMarkPos((CustomScanState *) node);
        PlanStateNode::CustomScan(m) => nodeCustom::ExecCustomMarkPos(m, estate),
        // case T_MaterialState: ExecMaterialMarkPos((MaterialState *) node);
        PlanStateNode::Material(m) => nodeMaterial::ExecMaterialMarkPos(m),
        // case T_SortState: ExecSortMarkPos((SortState *) node);
        PlanStateNode::Sort(m) => nodeSort::ExecSortMarkPos(m),
        // case T_ResultState: ExecResultMarkPos((ResultState *) node);
        PlanStateNode::Result(m) => nodeResult::ExecResultMarkPos(m, estate),

        // default:
        //   /* don't make hard error unless caller asks to restore... */
        //   elog(DEBUG2, "unrecognized node type: %d", (int) nodeTag(node));
        other => elog(DEBUG2, format!("unrecognized node type: {}", other.tag())),
    }
}

/// `ExecRestrPos(node)` — restores the scan position previously saved with
/// `ExecMarkPos()`.
///
/// NOTE: the semantics of this are that the first ExecProcNode following
/// the restore operation will yield the same tuple as the first one following
/// the mark operation.  It is unspecified what happens to the plan node's
/// result TupleTableSlot.  (In most cases the result slot is unchanged by
/// a restore, but the node may choose to clear it or to load it with the
/// restored-to tuple.)  Hence the caller should discard any previously
/// returned TupleTableSlot after doing a restore.
pub fn exec_restr_pos<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    match node {
        // case T_IndexScanState: ExecIndexRestrPos((IndexScanState *) node);
        PlanStateNode::IndexScan(m) => nodeIndexscan::ExecIndexRestrPos(m, estate),
        // case T_IndexOnlyScanState: ExecIndexOnlyRestrPos((IndexOnlyScanState *) node);
        PlanStateNode::IndexOnlyScan(m) => nodeIndexonlyscan::ExecIndexOnlyRestrPos(m, estate),
        // case T_CustomScanState: ExecCustomRestrPos((CustomScanState *) node);
        PlanStateNode::CustomScan(m) => nodeCustom::ExecCustomRestrPos(m, estate),
        // case T_MaterialState: ExecMaterialRestrPos((MaterialState *) node);
        PlanStateNode::Material(m) => nodeMaterial::ExecMaterialRestrPos(m),
        // case T_SortState: ExecSortRestrPos((SortState *) node);
        PlanStateNode::Sort(m) => nodeSort::ExecSortRestrPos(m),
        // case T_ResultState: ExecResultRestrPos((ResultState *) node);
        PlanStateNode::Result(m) => nodeResult::ExecResultRestrPos(m, estate),

        // default: elog(ERROR, "unrecognized node type: %d", (int) nodeTag(node));
        other => Err(unrecognized_node_type(other.tag())),
    }
}

/// `ExecSupportsMarkRestore(pathnode)` — does a Path support mark/restore?
///
/// This is used during planning and so must accept a Path, not a Plan.
/// We keep it here to be adjacent to the routines above, which also must
/// know which plan types support mark/restore.
pub fn exec_supports_mark_restore(pathnode: &PathNode<'_>) -> bool {
    // For consistency with the routines above, we do not examine the nodeTag
    // but rather the pathtype, which is the Plan node type the Path would
    // produce.
    match pathnode.path_head().pathtype {
        // Not all index types support mark/restore.
        //   return castNode(IndexPath, pathnode)->indexinfo->amcanmarkpos;
        T_IndexScan | T_IndexOnlyScan => match pathnode {
            PathNode::IndexPath(ip) => ip.indexinfo.amcanmarkpos,
            other => panic!("castNode(IndexPath, pathnode) failed: {other:?}"),
        },

        T_Material | T_Sort => true,

        T_CustomScan => match pathnode {
            PathNode::CustomPath(cp) => cp.flags & CUSTOMPATH_SUPPORT_MARK_RESTORE != 0,
            other => panic!("castNode(CustomPath, pathnode) failed: {other:?}"),
        },

        // Result supports mark/restore iff it has a child plan that does.
        //
        // We have to be careful here because there is more than one Path
        // type that can produce a Result plan node.
        T_Result => match pathnode {
            PathNode::ProjectionPath(pp) => exec_supports_mark_restore(&pp.subpath),
            PathNode::MinMaxAggPath(_) => false,  // childless Result
            PathNode::GroupResultPath(_) => false, // childless Result
            // Simple RTE_RESULT base relation: Assert(IsA(pathnode, Path))
            // (execAmi.c, T_Result default arm); childless Result.
            other => {
                debug_assert!(
                    matches!(other, PathNode::Path(_)),
                    "T_Result pathtype on unexpected Path node (C: Assert(IsA(pathnode, Path))): {other:?}"
                );
                false
            }
        },

        T_Append => match pathnode {
            PathNode::AppendPath(appendPath) => {
                // If there's exactly one child, then there will be no Append
                // in the final plan, so we can handle mark/restore if the
                // child plan node can.
                if appendPath.subpaths.len() == 1 {
                    exec_supports_mark_restore(&appendPath.subpaths[0])
                } else {
                    // Otherwise, Append can't handle it.
                    false
                }
            }
            other => panic!("castNode(AppendPath, pathnode) failed: {other:?}"),
        },

        T_MergeAppend => match pathnode {
            PathNode::MergeAppendPath(mapath) => {
                // Like the Append case above, single-subpath MergeAppends
                // won't be in the final plan, so just return the child's
                // mark/restore ability.
                if mapath.subpaths.len() == 1 {
                    exec_supports_mark_restore(&mapath.subpaths[0])
                } else {
                    // Otherwise, MergeAppend can't handle it.
                    false
                }
            }
            other => panic!("castNode(MergeAppendPath, pathnode) failed: {other:?}"),
        },

        _ => false,
    }
}

/// `ExecSupportsBackwardScan(node)` — does a plan type support backwards
/// scanning?
///
/// Ideally, all plan types would support backwards scan, but that seems
/// unlikely to happen soon.  In some cases, a plan node passes the backwards
/// scan down to its children, and so supports backwards scan only if its
/// children do.  Therefore, this routine must be passed a complete plan tree.
///
/// `None` is the C `node == NULL`.
pub fn exec_supports_backward_scan(node: Option<&Node<'_>>) -> PgResult<bool> {
    // if (node == NULL) return false;
    let Some(node) = node else {
        return Ok(false);
    };

    // Parallel-aware nodes return a subset of the tuples in each worker, and
    // in general we can't expect to have enough bookkeeping state to know
    // which ones we returned in this worker as opposed to some other worker.
    if node.plan_head().parallel_aware {
        return Ok(false);
    }

    match node.node_tag() {
        // case T_Result:
        //   if (outerPlan(node) != NULL)
        //       return ExecSupportsBackwardScan(outerPlan(node));
        //   else
        //       return false;
        ntag::T_Result => {
            match node.plan_head().lefttree.as_deref() {
                Some(outer) => exec_supports_backward_scan(Some(outer)),
                None => Ok(false),
            }
        }

        // case T_Append:
        //   /* With async, tuples may be interleaved, so can't back up. */
        //   if (((Append *) node)->nasyncplans > 0) return false;
        //   foreach(l, appendplans) if (!ExecSupportsBackwardScan(...)) return false;
        //   /* need not check tlist because Append doesn't evaluate it */
        //   return true;
        ntag::T_Append => {
            let append = node.expect_append();
            if append.nasyncplans > 0 {
                return Ok(false);
            }
            for child in &append.appendplans {
                if !exec_supports_backward_scan(Some(child))? {
                    return Ok(false);
                }
            }
            Ok(true)
        }

        // case T_Gather: return false;
        ntag::T_Gather => Ok(false),

        // case T_IndexScan:
        //   return IndexSupportsBackwardScan(((IndexScan *) node)->indexid);
        ntag::T_IndexScan => index_supports_backward_scan(node.expect_indexscan().indexid),

        // case T_IndexOnlyScan:
        //   return IndexSupportsBackwardScan(((IndexOnlyScan *) node)->indexid);
        ntag::T_IndexOnlyScan => index_supports_backward_scan(node.expect_indexonlyscan().indexid),

        // case T_SubqueryScan:
        //   return ExecSupportsBackwardScan(((SubqueryScan *) node)->subplan);
        ntag::T_SubqueryScan => {
            exec_supports_backward_scan(node.expect_subqueryscan().subplan.as_deref())
        }

        // case T_CustomScan:
        //   if (flags & CUSTOMPATH_SUPPORT_BACKWARD_SCAN) return true;
        //   return false;
        ntag::T_CustomScan => Ok(
            (node.expect_customscan().flags
                & types_nodes::nodeindexscan::CUSTOMPATH_SUPPORT_BACKWARD_SCAN)
                != 0,
        ),

        // case T_SeqScan / T_TidScan / T_TidRangeScan / T_FunctionScan /
        //      T_ValuesScan / T_CteScan / T_Material / T_Sort:
        //   /* these don't evaluate tlist */ return true;
        // (T_TidScan has no Node variant yet.)
        t if t == ntag::T_SeqScan
            || t == ntag::T_TidRangeScan
            || t == ntag::T_FunctionScan
            || t == ntag::T_ValuesScan
            || t == ntag::T_CteScan
            || t == ntag::T_Material
            || t == ntag::T_Sort =>
        {
            Ok(true)
        }

        // case T_Limit: return ExecSupportsBackwardScan(outerPlan(node));
        // (T_LockRows has no Node variant yet; T_IncrementalSort/T_SampleScan
        // return false in C, the wildcard default below covers them.)
        ntag::T_Limit => {
            exec_supports_backward_scan(node.plan_head().lefttree.as_deref())
        }

        // default: return false;
        _ => Ok(false),
    }
}

/// `IndexSupportsBackwardScan(indexid)` — an IndexScan or IndexOnlyScan node
/// supports backward scan only if the index's AM does.
///
/// Called from the IndexScan/IndexOnlyScan arms of
/// [`exec_supports_backward_scan`].
fn index_supports_backward_scan(indexid: Oid) -> PgResult<bool> {
    // Fetch the pg_class tuple of the index relation.
    //   ht_idxrel = SearchSysCache1(RELOID, ObjectIdGetDatum(indexid));
    //   if (!HeapTupleIsValid(ht_idxrel))
    //       elog(ERROR, "cache lookup failed for relation %u", indexid);
    //   idxrelrec = (Form_pg_class) GETSTRUCT(ht_idxrel);
    let relam = match syscache::search_relation_relam::call(indexid)? {
        Some(relam) => relam,
        None => {
            return Err(PgError::error(format!(
                "cache lookup failed for relation {indexid}"
            )))
        }
    };

    // Fetch the index AM's API struct and read amcanbackward (the installer
    // owns the pfree of the IndexAmRoutine and the ReleaseSysCache).
    //   amroutine = GetIndexAmRoutineByAmId(idxrelrec->relam, false);
    //   result = amroutine->amcanbackward;
    //   pfree(amroutine);
    //   ReleaseSysCache(ht_idxrel);
    amapi::index_am_canbackward::call(relam)
}

/// `ExecMaterializesOutput(plantype)` — does a plan type materialize its
/// output?
///
/// Returns true if the plan node type is one that automatically materializes
/// its output (typically by keeping it in a tuplestore).  For such plans,
/// a rescan without any parameter change will have zero startup cost and
/// very low per-tuple cost.
pub fn exec_materializes_output(plantype: NodeTag) -> bool {
    matches!(
        plantype,
        T_Material
            | T_FunctionScan
            | T_TableFuncScan
            | T_CteScan
            | T_NamedTuplestoreScan
            | T_WorkTableScan
            | T_Sort
    )
}

#[cfg(test)]
mod tests;
