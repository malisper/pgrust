//! The per-leg `ResultRelInfo` initialization blocks of
//! `ExecInitPartitionInfo` (executor/execPartition.c L501-985) that the CATALOG
//! standing-adjudicated as owned by nodeModifyTable: they read the
//! `ModifyTable` plan node (`withCheckOptionLists` / `returningLists` /
//! `onConflictAction` / `arbiterIndexes` / `onConflictSet` / `mergeActionLists`
//! / ...) and write ModifyTable-meaning `ResultRelInfo` fields
//! (`ri_WithCheckOptions`, `ri_returningList`, `ri_onConflictArbiterIndexes`,
//! `ri_onConflict`, `ri_MergeActions`, ...) for a leaf partition reached by
//! tuple routing.
//!
//! Each function owns the gating (the `if node && node->X != NIL` guard), the
//! reference-list selection (`linitial(node->...Lists)`), and — for ON CONFLICT
//! — the arbiter-index mapping loop and its length invariant. The genuinely
//! foreign callees (index open, attmap build + `map_variable_attnos` rewrite,
//! `ExecInitQual` / `ExecBuildProjectionInfo` / `ExecBuildUpdateProjection`,
//! `table_slot_create`, `get_partition_ancestors`, `RelationGetIndexList`) are
//! reached through their owner `-seams` crates.
//!
//! These install into the `backend-executor-nodeModifyTable-seams` declarations
//! (`exec_open_partition_indices` / `exec_init_partition_*`), which
//! execPartition's `ExecInitPartitionInfo` calls per leaf partition.

use mcx::Mcx;
use types_error::PgResult;
use types_core::primitive::Index;
use types_nodes::modifytable::{ONCONFLICT_NONE, ONCONFLICT_UPDATE};
use types_nodes::nodes::{CmdType, OnConflictAction};
use types_nodes::{EStateData, ModifyTableState, RriId};

extern crate alloc;

/// `((ModifyTable *) mtstate->ps.plan)->onConflictAction` — the ON CONFLICT
/// action of the `ModifyTableState`'s plan node, or `ONCONFLICT_NONE` when the
/// plan pointer is the C `NULL` (the `node ? ... : ONCONFLICT_NONE` guard).
/// Infallible — a plain field read.
pub fn ExecGetOnConflictAction(mtstate: &ModifyTableState<'_>) -> OnConflictAction {
    // ModifyTable *node = (ModifyTable *) mtstate->ps.plan;
    // node ? node->onConflictAction : ONCONFLICT_NONE
    mtstate
        .plan_node
        .map(|n| n.onConflictAction)
        .unwrap_or(ONCONFLICT_NONE)
}

/// The `relhasindex`-gated `ExecOpenIndices` leg of `ExecInitPartitionInfo`
/// (execPartition.c L543-547).
pub fn ExecOpenPartitionIndices<'mcx>(
    _mcx: Mcx<'mcx>,
    mtstate: &ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    leaf_part_rri: RriId,
) -> PgResult<()> {
    // ModifyTable *node = (ModifyTable *) mtstate->ps.plan;
    //
    // if (partrel->rd_rel->relhasindex &&
    //     leaf_part_rri->ri_IndexRelationDescs == NULL)
    //     ExecOpenIndices(leaf_part_rri,
    //                     (node != NULL &&
    //                      node->onConflictAction != ONCONFLICT_NONE));
    let rri = estate.result_rel(leaf_part_rri);
    let relhasindex = rri
        .ri_RelationDesc
        .as_ref()
        .map(|r| r.rd_rel.relhasindex)
        .unwrap_or(false);
    if relhasindex && rri.ri_IndexRelationDescs.is_none() {
        // speculative = (node != NULL && node->onConflictAction != ONCONFLICT_NONE)
        let speculative = mtstate
            .plan_node
            .map(|n| n.onConflictAction != ONCONFLICT_NONE)
            .unwrap_or(false);
        backend_executor_execIndexing_seams::exec_open_indices::call(
            estate,
            leaf_part_rri,
            speculative,
        )?;
    }
    Ok(())
}

/// The WITH CHECK OPTION leg of `ExecInitPartitionInfo` (execPartition.c
/// L549-614).
pub fn ExecInitPartitionWithCheckOptions<'mcx>(
    mcx: Mcx<'mcx>,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    leaf_part_rri: RriId,
    first_varno: Index,
    first_result_rel: RriId,
) -> PgResult<()> {
    // if (node && node->withCheckOptionLists != NIL) { ... }
    //
    // wcoList = linitial(node->withCheckOptionLists);  — the first plan's WCO
    // list is the reference; both that relation and this partition have the same
    // columns, so a simple varattno translation suffices.
    let node = match mtstate.plan_node {
        Some(n) => n,
        None => return Ok(()),
    };
    let ref_wco_list = match node.withCheckOptionLists.as_ref() {
        Some(lists) if !lists.is_empty() => lists[0].as_slice(),
        _ => return Ok(()),
    };

    // Convert Vars to the partition's attnos (build_attrmap_by_name +
    // map_variable_attnos), ExecInitQual each wco->qual, and store
    // ri_WithCheckOptions / ri_WithCheckOptionExprs — all execExpr/rewrite-owned.
    backend_executor_execExpr_seams::partition_init_with_check_options::call(
        mcx,
        mtstate,
        estate,
        leaf_part_rri,
        first_result_rel,
        first_varno,
        ref_wco_list,
    )
}

/// The RETURNING leg of `ExecInitPartitionInfo` (execPartition.c L616-679).
pub fn ExecInitPartitionReturning<'mcx>(
    mcx: Mcx<'mcx>,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    leaf_part_rri: RriId,
    first_varno: Index,
    first_result_rel: RriId,
) -> PgResult<()> {
    // if (node && node->returningLists != NIL) { ... }
    //
    // returningList = linitial(node->returningLists);  — the first plan's
    // RETURNING list is the reference.
    let node = match mtstate.plan_node {
        Some(n) => n,
        None => return Ok(()),
    };
    let ref_returning_list = match node.returningLists.as_ref() {
        Some(lists) if !lists.is_empty() => lists[0].as_slice(),
        _ => return Ok(()),
    };

    // Assert(mtstate->ps.ps_ResultTupleSlot != NULL);
    // Assert(mtstate->ps.ps_ExprContext != NULL);
    //
    // Map Vars to the partition's attnos and build ri_projectReturning via
    // ExecBuildProjectionInfo using the slot/econtext set up in
    // ExecInitModifyTable — all execExpr/rewrite-owned.
    backend_executor_execExpr_seams::partition_init_returning::call(
        mcx,
        mtstate,
        estate,
        leaf_part_rri,
        first_result_rel,
        first_varno,
        ref_returning_list,
    )
}

/// The ON CONFLICT leg of `ExecInitPartitionInfo` (execPartition.c L685-862).
pub fn ExecInitPartitionOnConflict<'mcx>(
    mcx: Mcx<'mcx>,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    leaf_part_rri: RriId,
    root_result_rel_info: RriId,
    first_varno: Index,
    first_result_rel: RriId,
) -> PgResult<()> {
    // if (node && node->onConflictAction != ONCONFLICT_NONE) { ... }
    let node = match mtstate.plan_node {
        Some(n) => n,
        None => return Ok(()),
    };
    if node.onConflictAction == ONCONFLICT_NONE {
        return Ok(());
    }

    // List *arbiterIndexes = NIL;
    //
    // If there is a list of arbiter indexes, map it to a list of indexes in the
    // partition by scanning the partition's index list and searching for
    // ancestry relationships to each index in the ancestor table.
    //   if (rootResultRelInfo->ri_onConflictArbiterIndexes != NIL) { ... }
    let mut arbiter_indexes: alloc::vec::Vec<types_core::Oid> = alloc::vec::Vec::new();

    let root_arbiters: alloc::vec::Vec<types_core::Oid> = estate
        .result_rel(root_result_rel_info)
        .ri_onConflictArbiterIndexes
        .as_ref()
        .map(|l| l.iter().copied().collect())
        .unwrap_or_default();

    if !root_arbiters.is_empty() {
        // childIdxs = RelationGetIndexList(leaf_part_rri->ri_RelationDesc);
        let part_rel = estate
            .result_rel(leaf_part_rri)
            .ri_RelationDesc
            .as_ref()
            .expect("ExecInitPartitionInfo: leaf partition ResultRelInfo has an open relation")
            .alias();
        let child_idxs = backend_utils_cache_relcache_seams::relation_get_index_list::call(
            mcx, &part_rel,
        )?;

        // foreach(lc, childIdxs) { ancestors = get_partition_ancestors(childIdx);
        //   foreach(lc2, root arbiters) if (list_member_oid(ancestors, lc2))
        //     arbiterIndexes = lappend_oid(arbiterIndexes, childIdx);
        //   list_free(ancestors); }
        for child_idx in child_idxs.iter().copied() {
            let ancestors =
                backend_catalog_partition_seams::get_partition_ancestors::call(mcx, child_idx)?;
            for root_arb in root_arbiters.iter().copied() {
                if ancestors.iter().any(|a| *a == root_arb) {
                    arbiter_indexes.push(child_idx);
                    break;
                }
            }
        }
    }

    // If the resulting lists are of inequal length, something is wrong.
    //   if (list_length(root arbiters) != list_length(arbiterIndexes))
    //       elog(ERROR, "invalid arbiter index list");
    if root_arbiters.len() != arbiter_indexes.len() {
        return Err(types_error::PgError::error("invalid arbiter index list"));
    }

    // leaf_part_rri->ri_onConflictArbiterIndexes = arbiterIndexes;
    {
        let dst = if arbiter_indexes.is_empty() {
            None
        } else {
            let mut v = mcx::vec_with_capacity_in(mcx, arbiter_indexes.len())?;
            for oid in arbiter_indexes.iter().copied() {
                v.push(oid);
            }
            Some(v)
        };
        estate.result_rel_mut(leaf_part_rri).ri_onConflictArbiterIndexes = dst;
    }

    // In the DO UPDATE case, we have some more state to initialize.
    //   if (node->onConflictAction == ONCONFLICT_UPDATE) { ... }
    if node.onConflictAction == ONCONFLICT_UPDATE {
        // Assert(node->onConflictSet != NIL);
        // Assert(rootResultRelInfo->ri_onConflict != NULL);
        let on_conflict_set = node
            .onConflictSet
            .as_ref()
            .map(|l| l.as_slice())
            .unwrap_or(&[]);
        let on_conflict_cols = node
            .onConflictCols
            .as_ref()
            .map(|l| l.as_slice())
            .unwrap_or(&[]);
        let on_conflict_where: Option<&[types_nodes::primnodes::Expr]> =
            node.onConflictWhere.as_deref();

        // Build the OnConflictSetState: oc_Existing (table_slot_create), and
        // either reuse the root's proj state (when ExecGetRootToChildMap is
        // NULL) or build a partition-specific UPDATE SET projection + WHERE
        // qual (map_variable_attnos over INNER_VAR then firstVarno,
        // adjust_partition_colnos, ExecBuildUpdateProjection, ExecInitQual) —
        // all execExpr/tableam/rewrite-owned.
        backend_executor_execExpr_seams::partition_init_on_conflict_update::call(
            mcx,
            mtstate,
            estate,
            leaf_part_rri,
            root_result_rel_info,
            first_result_rel,
            first_varno,
            on_conflict_set,
            on_conflict_cols,
            on_conflict_where,
        )?;
    }

    Ok(())
}

/// The MERGE leg of `ExecInitPartitionInfo` (execPartition.c L877-981).
pub fn ExecInitPartitionMerge<'mcx>(
    mcx: Mcx<'mcx>,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    leaf_part_rri: RriId,
    first_varno: Index,
    first_result_rel: RriId,
) -> PgResult<()> {
    let _ = first_result_rel;
    // if (node && node->operation == CMD_MERGE) { ... }
    let node = match mtstate.plan_node {
        Some(n) => n,
        None => return Ok(()),
    };
    if node.operation != CmdType::CMD_MERGE {
        return Ok(());
    }

    // List *firstMergeActionList = linitial(node->mergeActionLists);
    let ref_merge_action_list: &'mcx mcx::PgVec<'mcx, types_nodes::modifytable::MergeAction<'mcx>> =
        match node.mergeActionLists.as_ref() {
            Some(lists) if !lists.is_empty() => &lists[0],
            _ => return Ok(()),
        };

    // ExprContext *econtext = mtstate->ps.ps_ExprContext;
    let econtext = mtstate
        .ps
        .ps_ExprContext
        .expect("ExecInitPartitionInfo: MERGE node has an expression context");

    // joinCondition = linitial(node->mergeJoinConditions);
    let ref_join_condition: Option<&'mcx [types_nodes::primnodes::Expr]> = node
        .mergeJoinConditions
        .as_ref()
        .and_then(|jc| jc.first())
        .and_then(|c| c.as_ref())
        .map(|v| v.as_slice());

    // if (unlikely(!leaf_part_rri->ri_projectNewInfoValid))
    //     ExecInitMergeTupleSlots(mtstate, leaf_part_rri);
    if !estate.result_rel(leaf_part_rri).ri_projectNewInfoValid {
        crate::merge::ExecInitMergeTupleSlots(mcx, mtstate, estate, leaf_part_rri)?;
    }

    // Build the join-condition qual (ri_MergeJoinCondition) and each action's
    // MergeActionState (map_variable_attnos / adjust_partition_colnos_using_map,
    // ExecBuildProjectionInfo / ExecBuildUpdateProjection, ExecInitQual) into
    // ri_MergeActions[matchKind] — all execExpr/rewrite-owned.
    backend_executor_execExpr_seams::partition_init_merge_actions::call(
        mcx,
        mtstate,
        estate,
        leaf_part_rri,
        first_result_rel,
        first_varno,
        econtext,
        ref_join_condition,
        ref_merge_action_list.as_slice(),
    )
}
