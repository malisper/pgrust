//! MERGE family of `executor/nodeModifyTable.c`: the MERGE driver, the
//! not-matched action dispatch, and the per-action exec-state and tuple-slot
//! initialization. The matched-action dispatch (`ExecMergeMatched`) lives in
//! the [`crate::merge_matched`] sub-module.

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::{EStateData, ModifyTableState, RriId, SlotId};
use types_tuple::heaptuple::{HeapTuple, ItemPointerData};

use crate::ModifyTableContext;

/// `ExecMerge(context, resultRelInfo, tupleid, oldtuple, canSetTag)` — execute
/// the MERGE actions for one source row: dispatch to the matched or
/// not-matched action list depending on whether a target tuple
/// (`tupleid`/`oldtuple`) was found. Returns the RETURNING slot or `None`.
pub fn ExecMerge<'mcx>(
    mcx: Mcx<'mcx>,
    context: &mut ModifyTableContext,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    tupleid: Option<&ItemPointerData>,
    oldtuple: HeapTuple<'mcx>,
    can_set_tag: bool,
) -> PgResult<Option<SlotId>> {
    let mut rslot: Option<SlotId> = None;

    // matched = tupleid != NULL || oldtuple != NULL;
    let mut matched = tupleid.is_some() || oldtuple.is_some();
    if matched {
        rslot = crate::merge_matched::ExecMergeMatched(
            mcx,
            context,
            mtstate,
            estate,
            result_rel_info,
            tupleid,
            oldtuple,
            can_set_tag,
            &mut matched,
        )?;
    }

    // Deal with the NOT MATCHED case (either a NOT MATCHED tuple from the join,
    // or a previously MATCHED tuple for which ExecMergeMatched() set "matched"
    // to false, indicating that it no longer matches).
    if !matched {
        // If a concurrent update turned a MATCHED case into a NOT MATCHED case,
        // and we have both WHEN NOT MATCHED BY SOURCE and WHEN NOT MATCHED [BY
        // TARGET] actions, and there is a RETURNING clause, ExecMergeMatched()
        // may have already executed a WHEN NOT MATCHED BY SOURCE action, and
        // computed the row to return.  If so, we cannot execute a WHEN NOT
        // MATCHED [BY TARGET] action now, so mark it as pending (to be processed
        // on the next call to ExecModifyTable()).  Otherwise, just process the
        // action now.
        if rslot.is_none() {
            rslot = ExecMergeNotMatched(mcx, context, mtstate, estate, result_rel_info, can_set_tag)?;
        } else {
            mtstate.mt_merge_pending_not_matched = context.planSlot;
        }
    }

    Ok(rslot)
}

/// `ExecMergeNotMatched(context, resultRelInfo, canSetTag)` — run the WHEN NOT
/// MATCHED [BY TARGET] actions, performing the chosen INSERT/DO NOTHING.
/// Returns the RETURNING slot or `None`.
pub fn ExecMergeNotMatched<'mcx>(
    mcx: Mcx<'mcx>,
    context: &mut ModifyTableContext,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    can_set_tag: bool,
) -> PgResult<Option<SlotId>> {
    let _ = (mcx, can_set_tag, result_rel_info);

    // ExprContext *econtext = mtstate->ps.ps_ExprContext;
    let econtext = mtstate
        .ps
        .ps_ExprContext
        .expect("MERGE node has an expression context");

    // For INSERT actions, the root relation's merge action is OK since the
    // INSERT's targetlist and the WHEN conditions can only refer to the source
    // relation and hence it does not matter which result relation we work with.
    //
    // actionStates = resultRelInfo->ri_MergeActions[MERGE_WHEN_NOT_MATCHED_BY_TARGET];

    // Make source tuple available to ExecQual and ExecProject. We don't need the
    // target tuple, since the WHEN quals and targetlist can't refer to the
    // target columns.
    //
    //   econtext->ecxt_scantuple = NULL;
    //   econtext->ecxt_innertuple = context->planSlot;
    //   econtext->ecxt_outertuple = NULL;
    let plan_slot = context.planSlot;
    let ecxt = estate.ecxt_mut(econtext);
    ecxt.ecxt_scantuple = None;
    ecxt.ecxt_innertuple = plan_slot;
    ecxt.ecxt_outertuple = None;

    // foreach action in actionStates { ... }
    //
    // The WHEN NOT MATCHED [BY TARGET] action list lives in
    // ResultRelInfo.ri_MergeActions, a field owned by execMain that has not yet
    // been published into the shared types-nodes ResultRelInfo vocabulary, so
    // the per-action loop (ExecQual on mas_whenqual, ExecProject on mas_proj,
    // ExecInsert through rootResultRelInfo) cannot be reached.
    let _ = mtstate.mt_merge_inserted;
    panic!(
        "ExecMergeNotMatched: ResultRelInfo.ri_MergeActions (the WHEN NOT \
         MATCHED [BY TARGET] action list) is owned by execMain and not yet \
         present in the shared types-nodes ResultRelInfo vocabulary"
    );
}

/// `ExecInitMerge(mtstate, estate)` — build the per-action `MergeActionState`
/// list (projection + WHEN qual + per-rel slots) for every result relation's
/// merge action list.
pub fn ExecInitMerge<'mcx>(
    mcx: Mcx<'mcx>,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let _ = mcx;

    // List *mergeActionLists = mtstate->mt_mergeActionLists;
    //
    // if (mergeActionLists == NIL) return;
    let n_action_lists = match &mtstate.mt_mergeActionLists {
        None => return Ok(()),
        Some(lists) => lists.len(),
    };

    // mtstate->mt_merge_subcommands = 0;
    mtstate.mt_merge_subcommands = 0;

    // if (mtstate->ps.ps_ExprContext == NULL)
    //     ExecAssignExprContext(estate, &mtstate->ps);
    // econtext = mtstate->ps.ps_ExprContext;
    if mtstate.ps.ps_ExprContext.is_none() {
        backend_executor_execUtils_seams::exec_assign_expr_context::call(estate, &mut mtstate.ps)?;
    }

    // Create a MergeActionState for each action on the mergeActionList and add
    // it to either a list of matched actions or not-matched actions.
    //
    // The per-rel loop builds, for every result relation:
    //   - the MERGE fetch slots (ExecInitMergeTupleSlots, gated on
    //     ResultRelInfo.ri_projectNewInfoValid);
    //   - the join-condition ExprState (ResultRelInfo.ri_MergeJoinCondition);
    //   - a MergeActionState per action, appended into
    //     ResultRelInfo.ri_MergeActions[action->matchKind], carrying the
    //     compiled WHEN qual (ExecInitQual) and the action projection
    //     (ExecBuildProjectionInfo for INSERT / ExecBuildUpdateProjection for
    //     UPDATE);
    // and finally the root relation's WITH CHECK OPTION / RETURNING setup when
    // the MERGE targets an inherited (non-partitioned) table with INSERT
    // actions.
    //
    // All of those ResultRelInfo fields (ri_MergeActions, ri_MergeJoinCondition,
    // ri_newTupleSlot/ri_oldTupleSlot/ri_projectNewInfoValid,
    // ri_WithCheckOptions/ri_WithCheckOptionExprs, ri_returningList/
    // ri_projectReturning) are owned by execMain and have not yet been published
    // into the shared types-nodes ResultRelInfo vocabulary, and the per-action
    // projection builders (ExecBuildProjectionInfo / ExecBuildUpdateProjection
    // over an explicit target list and slot/desc) are not yet declared on the
    // execExpr seam crate, so the per-rel loop body cannot be reached.
    let _ = (n_action_lists, &mtstate.mt_mergeJoinConditions, mtstate.rootResultRelInfo);
    panic!(
        "ExecInitMerge: ResultRelInfo.ri_MergeActions/ri_MergeJoinCondition/\
         ri_newTupleSlot/ri_WithCheckOptions/ri_returningList are owned by \
         execMain and the per-action ExecBuildProjectionInfo/\
         ExecBuildUpdateProjection seams are not yet declared on execExpr; \
         neither is present in the shared vocabulary"
    );
}

/// `ExecInitMergeTupleSlots(mtstate, resultRelInfo)` — create the
/// existing/output tuple slots a relation's MERGE actions project into.
pub fn ExecInitMergeTupleSlots<'mcx>(
    mcx: Mcx<'mcx>,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
) -> PgResult<()> {
    let _ = mtstate;

    // EState *estate = mtstate->ps.state;  — supplied as a parameter here.
    //
    // Assert(!resultRelInfo->ri_projectNewInfoValid);
    debug_assert!(!estate.result_rel(result_rel_info).ri_projectNewInfoValid);

    // resultRelInfo->ri_oldTupleSlot =
    //     table_slot_create(resultRelInfo->ri_RelationDesc, &estate->es_tupleTable);
    // resultRelInfo->ri_newTupleSlot =
    //     table_slot_create(resultRelInfo->ri_RelationDesc, &estate->es_tupleTable);
    // resultRelInfo->ri_projectNewInfoValid = true;
    let rel = estate
        .result_rel(result_rel_info)
        .ri_RelationDesc
        .as_ref()
        .expect("MERGE target ResultRelInfo has an open relation")
        .alias();
    let old_slot = backend_access_table_tableam::table_slot_create(mcx, &rel)?;
    let old_id = estate.make_slot(old_slot)?;
    let new_slot = backend_access_table_tableam::table_slot_create(mcx, &rel)?;
    let new_id = estate.make_slot(new_slot)?;
    let rri = estate.result_rel_mut(result_rel_info);
    rri.ri_oldTupleSlot = Some(old_id);
    rri.ri_newTupleSlot = Some(new_id);
    rri.ri_projectNewInfoValid = true;

    Ok(())
}
