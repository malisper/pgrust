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
use backend_executor_nodeMaterial as nodeMaterial;
use backend_executor_nodeSubplan_seams as nodeSubplan;
use backend_utils_cache_syscache_seams as syscache;
use backend_utils_error::elog;
use types_core::{NodeTag, Oid};
use types_error::{PgError, PgResult, DEBUG2};
use types_nodes::nodeindexscan::CUSTOMPATH_SUPPORT_MARK_RESTORE;
use types_nodes::nodes::{
    Node, T_CteScan, T_FunctionScan, T_IndexOnlyScan, T_IndexScan, T_Material, T_MergeAppend,
    T_NamedTuplestoreScan, T_Result, T_Sort, T_TableFuncScan, T_WorkTableScan, T_Append,
    T_CustomScan,
};
use types_nodes::pathnodes::PathNode;
use types_nodes::{EStateData, PlanStateNode};

/// Install this crate's implementations into its seam slots.
pub fn init_seams() {
    backend_executor_execAmi_seams::exec_re_scan::set(exec_re_scan);
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
                .and_then(|splan| splan.ps_head().plan.as_deref())
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
                .and_then(|splan| splan.ps_head().plan.as_deref())
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
    match node {
        // case T_MaterialState: ExecReScanMaterial(...)
        PlanStateNode::Material(m) => nodeMaterial::ExecReScanMaterial(m, estate)?,

        // The remaining C arms (ResultState ... LimitState) gain match arms as
        // their node-state variants are added to PlanStateNode; until then the
        // only reachable wildcard case is the C default:
        //   elog(ERROR, "unrecognized node type: %d", (int) nodeTag(node));
        other => return Err(unrecognized_node_type(other.tag())),
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
pub fn exec_mark_pos(node: &mut PlanStateNode<'_>) -> PgResult<()> {
    match node {
        // case T_MaterialState: ExecMaterialMarkPos(...)
        PlanStateNode::Material(m) => nodeMaterial::ExecMaterialMarkPos(m),

        // The IndexScan/IndexOnlyScan/CustomScan/Sort/Result arms gain match
        // arms as their state variants land. C default:
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
pub fn exec_restr_pos(node: &mut PlanStateNode<'_>) -> PgResult<()> {
    match node {
        // case T_MaterialState: ExecMaterialRestrPos(...)
        PlanStateNode::Material(m) => nodeMaterial::ExecMaterialRestrPos(m),

        // The IndexScan/IndexOnlyScan/CustomScan/Sort/Result arms gain match
        // arms as their state variants land. C default: elog(ERROR, ...).
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

    match node {
        // case T_Material (with T_SeqScan, T_TidScan, T_TidRangeScan,
        // T_FunctionScan, T_ValuesScan, T_CteScan, T_Sort):
        //   /* these don't evaluate tlist */ return true;
        Node::Material(_) => Ok(true),

        // The remaining C arms (Result, Append, SampleScan, Gather,
        // IndexScan/IndexOnlyScan via index_supports_backward_scan,
        // SubqueryScan, CustomScan, IncrementalSort, LockRows, Limit) gain
        // match arms as their plan-node variants are added to Node; until
        // then the wildcard is the C default: return false.
        _ => Ok(false),
    }
}

/// `IndexSupportsBackwardScan(indexid)` — an IndexScan or IndexOnlyScan node
/// supports backward scan only if the index's AM does.
///
/// Unreachable until the IndexScan/IndexOnlyScan plan-node variants land in
/// `Node` (their `exec_supports_backward_scan` arms call this), hence
/// `dead_code` outside tests.
#[cfg_attr(not(test), allow(dead_code))]
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
