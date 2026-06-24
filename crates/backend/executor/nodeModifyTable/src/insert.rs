//! INSERT family of `executor/nodeModifyTable.c`: the batched insert paths,
//! INSERT ... ON CONFLICT arbitration, and the tuple-visibility checks ON
//! CONFLICT relies on. The single-tuple `ExecInsert` driver lives in the
//! [`crate::insert_exec`] sub-module.

use ::mcx::Mcx;
use ::types_core::xact::CommandId;
use ::types_error::{
    PgError, PgResult, ERRCODE_CARDINALITY_VIOLATION, ERRCODE_T_R_SERIALIZATION_FAILURE,
};
use ::nodes::{EStateData, ModifyTableState, RriId, SlotId};
use ::rel::Relation;
use ::types_tableam::tableam::{
    LockTupleMode, Snapshot, TM_FailureData, TM_Result,
};
use ::types_tuple::heaptuple::{ItemPointerData, MinTransactionIdAttributeNumber};

use crate::lifecycle::ExecCheckPlanOutput;
use crate::update::ExecUpdate;
use crate::ModifyTableContext;

/// `WCO_VIEW_CHECK` (parsenodes.h `WCOKind`): WCO on an auto-updatable view.
const WCO_VIEW_CHECK: i32 = 0;
/// `WCO_RLS_CONFLICT_CHECK` (parsenodes.h `WCOKind`): RLS ON CONFLICT DO UPDATE
/// USING policy.
const WCO_RLS_CONFLICT_CHECK: i32 = 3;

// ---------------------------------------------------------------------------
// Seams into unported neighbors that the INSERT family reaches. Each is a thin
// marshal+delegate slot the owner installs when it lands; until then a call
// panics with the seam's path (AGENTS.md: loud panic over a silent stub).
// These mirror the C `ResultRelInfo`/`EState`/slot/FDW-vtable fields and the
// trigger/WCO/table-AM/snapshot routines the node layer drives across a
// dependency cycle, keyed by the owned-model `RriId`/`SlotId` handles like the
// rest of this crate's seams.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `resultRelInfo->ri_FdwRoutine->ExecForeignBatchInsert(estate,
    /// resultRelInfo, slots, planSlots, &numInserted)` (foreign-data wrapper
    /// vtable): flush a batch of buffered FDW inserts; returns the result slots
    /// actually inserted (its length is the C `numInserted` out-parameter).
    pub fn exec_foreign_batch_insert<'mcx>(
        estate: &mut EStateData<'mcx>,
        result_rel_info: RriId,
        slots: &[SlotId],
        plan_slots: &[SlotId],
    ) -> PgResult<::mcx::PgVec<'mcx, SlotId>>
);

seam_core::seam!(
    /// `slot->tts_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc)`
    /// (tuptable.h): set the slot's reported table OID before firing AFTER ROW
    /// triggers / RETURNING (which may reference tableoid).
    pub fn slot_set_table_oid<'mcx>(
        estate: &mut EStateData<'mcx>,
        slot: SlotId,
        relid: ::types_core::Oid,
    )
);

// `ExecARInsertTriggers` is declared in `backend-commands-trigger-seams`
// (the trigger owner). The FDW-batch call below extracts the transition-capture
// state from `mtstate` (the C `mt_transition_capture`) and passes it explicitly,
// matching the shared seam's signature.

seam_core::seam!(
    /// `ExecClearTuple(slot)` (execTuples.c): clear the slot's tuple.
    pub fn exec_clear_tuple<'mcx>(estate: &mut EStateData<'mcx>, slot: SlotId) -> PgResult<()>
);

seam_core::seam!(
    /// `RelationGetRelid(resultRelInfo->ri_RelationDesc)` (rel.h): the target
    /// relation's OID.
    pub fn ri_relation_relid(estate: &EStateData<'_>, result_rel_info: RriId) -> ::types_core::Oid
);

seam_core::seam!(
    /// `resultRelInfo->ri_WithCheckOptions != NIL` (execnodes.h): does the
    /// result relation carry WITH CHECK OPTION / RLS policies?
    pub fn ri_has_with_check_options(estate: &EStateData<'_>, result_rel_info: RriId) -> bool
);

seam_core::seam!(
    /// `resultRelInfo->ri_needLockTagTuple` (execnodes.h): does this result
    /// relation need a tuple-level heavyweight lock around the update?
    pub fn ri_need_lock_tag_tuple(estate: &EStateData<'_>, result_rel_info: RriId) -> bool
);

seam_core::seam!(
    /// `ExecWithCheckOptions(kind, resultRelInfo, slot, estate)` (execMain.c):
    /// evaluate the WCO/RLS policies of the given `kind` on `slot`,
    /// `ereport(ERROR)`ing on a violation.
    pub fn exec_with_check_options<'mcx>(
        estate: &mut EStateData<'mcx>,
        kind: i32,
        result_rel_info: RriId,
        slot: SlotId,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `resultRelInfo->ri_Slots` (execnodes.h): the buffered-insert source
    /// slots accumulated for this result relation's pending FDW batch.
    pub fn ri_slots<'mcx>(estate: &EStateData<'mcx>, result_rel_info: RriId) -> ::mcx::PgVec<'mcx, SlotId>
);

seam_core::seam!(
    /// `resultRelInfo->ri_PlanSlots` (execnodes.h): the buffered-insert plan
    /// slots paralleling `ri_Slots`.
    pub fn ri_plan_slots<'mcx>(estate: &EStateData<'mcx>, result_rel_info: RriId) -> ::mcx::PgVec<'mcx, SlotId>
);

seam_core::seam!(
    /// `resultRelInfo->ri_NumSlots` (execnodes.h): count of buffered insert
    /// slots; reset to 0 once the batch is flushed.
    pub fn ri_num_slots(estate: &EStateData<'_>, result_rel_info: RriId) -> i32
);

seam_core::seam!(
    /// `resultRelInfo->ri_NumSlots = n` (execnodes.h): set the buffered-insert
    /// slot count (the flush epilogue resets it to 0).
    pub fn ri_set_num_slots<'mcx>(estate: &mut EStateData<'mcx>, result_rel_info: RriId, n: i32)
);

seam_core::seam!(
    /// `estate->es_insert_pending_modifytables` lfirst → the buffered
    /// relation's owning `ModifyTableState` (an `Opaque` alias in the owned
    /// EState): downcast the pending-modifytable entry at `idx` back to a
    /// `&mut ModifyTableState` and run `ExecBatchInsert` against it. Until the
    /// EState owner lands the typed back-reference, this is the only way to
    /// reach the aliased mtstate.
    pub fn flush_pending_insert<'mcx>(
        estate: &mut EStateData<'mcx>,
        idx: usize,
        result_rel_info: RriId,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `relinfo->ri_projectNew == NULL` (execnodes.h): is the insert
    /// "new tuple" projection unset (the common INSERT case)?
    pub fn ri_project_new_is_null(estate: &EStateData<'_>, relinfo: RriId) -> bool
);

seam_core::seam!(
    /// `relinfo->ri_newTupleSlot->tts_ops != planSlot->tts_ops` (tuptable.h):
    /// does the relation's new-tuple slot use a different slot class than the
    /// plan slot (so the plan slot can't be used as-is)?
    pub fn ri_new_tuple_slot_ops_differ<'mcx>(
        estate: &EStateData<'mcx>,
        relinfo: RriId,
        plan_slot: SlotId,
    ) -> bool
);

seam_core::seam!(
    /// `ExecCopySlot(relinfo->ri_newTupleSlot, planSlot)` (execTuples.c): copy
    /// `plan_slot` into the relation's new-tuple slot, returning that slot id.
    pub fn exec_copy_into_new_tuple_slot<'mcx>(
        estate: &mut EStateData<'mcx>,
        relinfo: RriId,
        plan_slot: SlotId,
    ) -> PgResult<SlotId>
);

seam_core::seam!(
    /// `econtext = newProj->pi_exprContext; econtext->ecxt_outertuple =
    /// planSlot; return ExecProject(newProj)` (execExpr.c) where `newProj` is
    /// `relinfo->ri_projectNew`: project the plan slot through the insert
    /// new-tuple projection, returning the projected slot.
    pub fn exec_project_insert_new_tuple<'mcx>(
        estate: &mut EStateData<'mcx>,
        relinfo: RriId,
        plan_slot: SlotId,
    ) -> PgResult<SlotId>
);

seam_core::seam!(
    /// `resultRelInfo->ri_projectNew = ExecBuildProjectionInfo(insertTargetList,
    /// mtstate->ps.ps_ExprContext, resultRelInfo->ri_newTupleSlot, &mtstate->ps,
    /// relDesc)` (execExpr.c): build the junk-filtered insert projection into
    /// `ri_projectNew` over the supplied non-junk target list (the execExpr
    /// owner's `ExecBuildProjectionInfo` seam reads `planstate->plan->targetlist`
    /// directly, so the junk-filtered list this routine derives needs its own
    /// entry point). Allocates in the EState per-query context.
    pub fn exec_build_insert_projection<'mcx>(
        estate: &mut EStateData<'mcx>,
        mtstate: &mut ModifyTableState<'mcx>,
        result_rel_info: RriId,
        insert_target_list: &[::nodes::primnodes::TargetEntry<'mcx>],
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `IsolationUsesXactSnapshot()` (xact.h): is the current isolation level
    /// REPEATABLE READ or SERIALIZABLE (transaction-snapshot mode)?
    pub fn isolation_uses_xact_snapshot() -> bool
);

seam_core::seam!(
    /// `EState.es_snapshot` (execnodes.h): the EState's active scan snapshot,
    /// passed explicitly to `table_tuple_lock` until execMain owns it.
    pub fn es_snapshot(estate: &EStateData<'_>) -> Snapshot
);


seam_core::seam!(
    /// `table_tuple_lock(rel, tid, snapshot, slot, cid, mode, LockWaitBlock, 0,
    /// tmfd)` (tableam.h): lock the conflicting tuple into `slot` with the given
    /// mode, blocking on contention (`wait_policy` LockWaitBlock, no flags).
    /// Returns the lock outcome.
    pub fn table_tuple_lock<'mcx>(
        estate: &mut EStateData<'mcx>,
        result_rel_info: RriId,
        tupleid: &ItemPointerData,
        snapshot: Snapshot,
        slot: SlotId,
        cid: CommandId,
        mode: LockTupleMode,
        tmfd: &mut TM_FailureData,
    ) -> PgResult<TM_Result>
);

seam_core::seam!(
    /// `table_tuple_fetch_row_version(rel, tid, SnapshotAny, slot)`
    /// (tableam.h): fetch the row version identified by `tid` into `slot` under
    /// SnapshotAny. Returns `false` if no tuple was found.
    pub fn table_tuple_fetch_row_version_any<'mcx>(
        estate: &mut EStateData<'mcx>,
        result_rel_info: RriId,
        tupleid: &ItemPointerData,
        slot: SlotId,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `table_tuple_satisfies_snapshot(rel, slot, snapshot)` (tableam.h): does
    /// the tuple in `slot` satisfy `snapshot`?
    pub fn table_tuple_satisfies_snapshot<'mcx>(
        estate: &mut EStateData<'mcx>,
        rel: Relation<'mcx>,
        slot: SlotId,
        snapshot: Snapshot,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `slot_getsysattr(slot, MinTransactionIdAttributeNumber, &isnull)`
    /// (tuptable.h) followed by `DatumGetTransactionId`: fetch the slot tuple's
    /// `xmin` as `(xmin, isnull)`.
    pub fn slot_get_xmin<'mcx>(
        estate: &mut EStateData<'mcx>,
        slot: SlotId,
    ) -> PgResult<(::types_core::TransactionId, bool)>
);

seam_core::seam!(
    /// `econtext->ecxt_scantuple = existing; econtext->ecxt_innertuple =
    /// excludedSlot; econtext->ecxt_outertuple = NULL` (execnodes.h): install
    /// the ON CONFLICT existing/EXCLUDED tuples into `mtstate`'s expression
    /// context before evaluating the SET WHERE clause and projection.
    pub fn oc_set_econtext_tuples<'mcx>(
        estate: &mut EStateData<'mcx>,
        mtstate: &mut ModifyTableState<'mcx>,
        existing: SlotId,
        excluded_slot: SlotId,
    )
);

seam_core::seam!(
    /// `resultRelInfo->ri_onConflict->oc_Existing` (execnodes.h): the slot used
    /// to hold the conflicting (existing) tuple during ON CONFLICT DO UPDATE.
    pub fn oc_existing_slot<'mcx>(estate: &EStateData<'mcx>, result_rel_info: RriId) -> SlotId
);

seam_core::seam!(
    /// `ExecQual(resultRelInfo->ri_onConflict->oc_WhereClause, econtext)`
    /// (execExpr.c): evaluate the ON CONFLICT DO UPDATE SET WHERE clause against
    /// `mtstate`'s expression context. Returns whether the qual passed.
    pub fn exec_qual_oc_where<'mcx>(
        estate: &mut EStateData<'mcx>,
        mtstate: &mut ModifyTableState<'mcx>,
        result_rel_info: RriId,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `InstrCountFiltered1(&mtstate->ps, 1)` (instrument.h): count one row
    /// filtered out by the ON CONFLICT SET WHERE clause.
    pub fn instr_count_filtered1<'mcx>(mtstate: &mut ModifyTableState<'mcx>, n: u64)
);

seam_core::seam!(
    /// `ExecProject(resultRelInfo->ri_onConflict->oc_ProjInfo)` (execExpr.c):
    /// project the new tuple version into `oc_ProjSlot`, returning that slot id.
    pub fn exec_project_oc<'mcx>(
        estate: &mut EStateData<'mcx>,
        result_rel_info: RriId,
    ) -> PgResult<SlotId>
);

seam_core::seam!(
    /// `resultRelInfo->ri_onConflict->oc_ProjSlot` (execnodes.h): the slot the
    /// ON CONFLICT SET projection writes into and that `ExecUpdate` applies.
    pub fn oc_proj_slot<'mcx>(estate: &EStateData<'mcx>, result_rel_info: RriId) -> SlotId
);

seam_core::seam!(
    /// `*returning != NULL && resultRelInfo->ri_projectReturning->pi_state.flags
    /// & EEO_FLAG_HAS_OLD` (execnodes.h/execExpr.h): does the RETURNING
    /// projection reference OLD columns (so the returning slot must be
    /// materialized before the existing tuple is cleared)?
    pub fn ri_returning_has_old(estate: &EStateData<'_>, result_rel_info: RriId) -> bool
);

seam_core::seam!(
    /// `ExecMaterializeSlot(slot)` (execTuples.c): force the slot to hold a
    /// local copy of any pass-by-reference values.
    pub fn exec_materialize_slot<'mcx>(estate: &mut EStateData<'mcx>, slot: SlotId) -> PgResult<()>
);

/// `ExecBatchInsert(mtstate, resultRelInfo, slots, planSlots, numSlots,
/// estate, canSetTag)` — flush a batch of pending FDW inserts via
/// `ExecForeignBatchInsert`, firing AFTER ROW triggers and RETURNING for each.
pub fn ExecBatchInsert<'mcx>(
    _mcx: Mcx<'mcx>,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    slots: &mut [SlotId],
    plan_slots: &mut [SlotId],
    num_slots: i32,
    can_set_tag: bool,
) -> PgResult<()> {
    // int numInserted = numSlots;
    //
    // rslots = resultRelInfo->ri_FdwRoutine->ExecForeignBatchInsert(estate,
    //              resultRelInfo, slots, planSlots, &numInserted);
    let rslots = exec_foreign_batch_insert::call(estate, result_rel_info, slots, plan_slots)?;
    let num_inserted = rslots.len();

    // for (i = 0; i < numInserted; i++)
    for i in 0..num_inserted {
        // slot = rslots[i];
        let slot = rslots[i];

        // AFTER ROW Triggers might reference the tableoid column, so
        // (re-)initialize tts_tableOid before evaluating them.
        // slot->tts_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);
        let relid = ri_relation_relid::call(estate, result_rel_info);
        slot_set_table_oid::call(estate, slot, relid);

        // AFTER ROW INSERT Triggers
        trigger_seams::exec_ar_insert_triggers::call(
            estate,
            result_rel_info,
            slot,
            &[],
            mtstate.mt_transition_capture.as_deref_mut(),
        )?;

        // Check any WITH CHECK OPTION constraints from parent views.  See the
        // comment in ExecInsert.
        // if (resultRelInfo->ri_WithCheckOptions != NIL)
        if ri_has_with_check_options::call(estate, result_rel_info) {
            exec_with_check_options::call(estate, WCO_VIEW_CHECK, result_rel_info, slot)?;
        }
    }

    // if (canSetTag && numInserted > 0)
    //     estate->es_processed += numInserted;
    if can_set_tag && num_inserted > 0 {
        estate.es_processed += num_inserted as u64;
    }

    // Clean up all the slots, ready for the next batch.
    // for (i = 0; i < numSlots; i++) { ExecClearTuple(slots[i]); ExecClearTuple(planSlots[i]); }
    let n = num_slots as usize;
    for i in 0..n {
        exec_clear_tuple::call(estate, slots[i])?;
        exec_clear_tuple::call(estate, plan_slots[i])?;
    }

    // resultRelInfo->ri_NumSlots = 0;
    ri_set_num_slots::call(estate, result_rel_info, 0);

    Ok(())
}

/// `ExecPendingInserts(estate)` — flush every result relation that has pending
/// batched FDW inserts (`es_insert_pending_result_relations`).
pub fn ExecPendingInserts<'mcx>(
    _mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // forboth(l1, estate->es_insert_pending_result_relations,
    //         l2, estate->es_insert_pending_modifytables) { ... }
    //
    // The two lists are walked in lockstep. Each pending result relation's
    // ExecBatchInsert runs against the owning ModifyTableState aliased in
    // es_insert_pending_modifytables (an Opaque entry in the owned EState); the
    // flush_pending_insert seam downcasts that alias and dispatches
    // ExecBatchInsert with the relation's ri_Slots/ri_PlanSlots/ri_NumSlots and
    // mtstate->canSetTag, exactly as the C does.
    let pending: Vec<RriId> = estate
        .es_insert_pending_result_relations
        .iter()
        .copied()
        .collect();
    for (idx, result_rel_info) in pending.into_iter().enumerate() {
        // Assert(mtstate);
        flush_pending_insert::call(estate, idx, result_rel_info)?;
    }

    // list_free(estate->es_insert_pending_result_relations);
    // list_free(estate->es_insert_pending_modifytables);
    // estate->es_insert_pending_result_relations = NIL;
    // estate->es_insert_pending_modifytables = NIL;
    estate.es_insert_pending_result_relations.clear();
    estate.es_insert_pending_modifytables.clear();

    Ok(())
}

/// `ExecGetInsertNewTuple(relinfo, planSlot)` — project the subplan tuple
/// through the insert "new tuple" projection (`ri_projectNew`), returning the
/// stored-relation-rowtype slot. Returns `planSlot` unchanged when no
/// projection is needed.
pub fn ExecGetInsertNewTuple<'mcx>(
    estate: &mut EStateData<'mcx>,
    relinfo: RriId,
    plan_slot: SlotId,
) -> PgResult<SlotId> {
    // ProjectionInfo *newProj = relinfo->ri_projectNew;
    //
    // If there's no projection to be done, just make sure the slot is of the
    // right type for the target rel.  If the planSlot is the right type we can
    // use it as-is, else copy the data into ri_newTupleSlot.
    if ri_project_new_is_null::call(estate, relinfo) {
        // if (relinfo->ri_newTupleSlot->tts_ops != planSlot->tts_ops)
        if ri_new_tuple_slot_ops_differ::call(estate, relinfo, plan_slot) {
            // ExecCopySlot(relinfo->ri_newTupleSlot, planSlot);
            // return relinfo->ri_newTupleSlot;
            return exec_copy_into_new_tuple_slot::call(estate, relinfo, plan_slot);
        } else {
            // return planSlot;
            return Ok(plan_slot);
        }
    }

    // Else project; since the projection output slot is ri_newTupleSlot, this
    // will also fix any slot-type problem.
    //
    // Note: currently, this is dead code, because INSERT cases don't receive
    // any junk columns so there's never a projection to be done.
    //
    // econtext = newProj->pi_exprContext;
    // econtext->ecxt_outertuple = planSlot;
    // return ExecProject(newProj);
    exec_project_insert_new_tuple::call(estate, relinfo, plan_slot)
}

/// `ExecInitInsertProjection(mtstate, resultRelInfo)` — build the insert
/// projection (`ri_projectNew` + `ri_newTupleSlot`) that maps the subplan
/// output to the target relation's rowtype.
pub fn ExecInitInsertProjection<'mcx>(
    mcx: Mcx<'mcx>,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
) -> PgResult<()> {
    // ModifyTable *node = (ModifyTable *) mtstate->ps.plan;
    // Plan *subplan = outerPlan(node);
    //
    // Extract non-junk columns of the subplan's result tlist; set
    // need_projection when any junk column is present.
    //   foreach(l, subplan->targetlist) {
    //       if (!tle->resjunk) insertTargetList = lappend(insertTargetList, tle);
    //       else need_projection = true;
    //   }
    let (insert_target_list, need_projection) = {
        let node_plan = mtstate
            .ps
            .plan
            .expect("ExecInitInsertProjection: mtstate->ps.plan is NULL");
        let subplan = node_plan
            .outer_plan()
            .expect("ExecInitInsertProjection: outerPlan(node) is NULL");
        let empty: &[::nodes::primnodes::TargetEntry<'_>] = &[];
        let subplan_tlist = subplan
            .plan_head()
            .targetlist
            .as_ref()
            .map(|tl| tl.as_slice())
            .unwrap_or(empty);

        let mut list = ::mcx::vec_with_capacity_in(mcx, subplan_tlist.len())?;
        let mut need = false;
        for tle in subplan_tlist.iter() {
            if !tle.resjunk {
                list.push(tle.clone_in(mcx)?);
            } else {
                need = true;
            }
        }
        (list, need)
    };

    // The junk-free list must produce a tuple suitable for the result relation.
    //   ExecCheckPlanOutput(resultRelInfo->ri_RelationDesc, insertTargetList);
    {
        let rel = estate
            .result_rel(result_rel_info)
            .ri_RelationDesc
            .as_ref()
            .expect("ExecInitInsertProjection: result relation must be open")
            .alias();
        ExecCheckPlanOutput(rel, insert_target_list.as_slice())?;
    }

    // We'll need a slot matching the table's format.
    //   resultRelInfo->ri_newTupleSlot =
    //       table_slot_create(resultRelInfo->ri_RelationDesc, &estate->es_tupleTable);
    let rel = estate
        .result_rel(result_rel_info)
        .ri_RelationDesc
        .as_ref()
        .expect("ExecInitInsertProjection: result relation must be open")
        .alias();
    let new_tuple_slot = table_tableam::table_slot_create(mcx, &rel)?;
    let new_id = estate.push_slot_data(new_tuple_slot)?;
    estate.result_rel_mut(result_rel_info).ri_newTupleSlot = Some(new_id);

    // Build ProjectionInfo if needed (it probably isn't).
    //   if (need_projection) {
    //       if (mtstate->ps.ps_ExprContext == NULL)
    //           ExecAssignExprContext(estate, &mtstate->ps);
    //       resultRelInfo->ri_projectNew = ExecBuildProjectionInfo(insertTargetList,
    //           mtstate->ps.ps_ExprContext, resultRelInfo->ri_newTupleSlot,
    //           &mtstate->ps, relDesc);
    //   }
    if need_projection {
        // need an expression context to do the projection
        if mtstate.ps.ps_ExprContext.is_none() {
            execUtils_seams::exec_assign_expr_context::call(
                estate,
                &mut mtstate.ps,
            )?;
        }
        exec_build_insert_projection::call(
            estate,
            mtstate,
            result_rel_info,
            insert_target_list.as_slice(),
        )?;
    }

    // resultRelInfo->ri_projectNewInfoValid = true;
    estate.result_rel_mut(result_rel_info).ri_projectNewInfoValid = true;

    Ok(())
}

/// `ExecOnConflictUpdate(context, resultRelInfo, conflictTid, excludedSlot,
/// canSetTag, returning)` — handle the DO UPDATE branch of INSERT ... ON
/// CONFLICT. Returns `false` when the caller must retry the INSERT from
/// scratch.
pub fn ExecOnConflictUpdate<'mcx>(
    mcx: Mcx<'mcx>,
    context: &mut ModifyTableContext,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    conflict_tid: &ItemPointerData,
    excluded_slot: SlotId,
    can_set_tag: bool,
    returning: &mut Option<SlotId>,
) -> PgResult<bool> {
    // Relation relation = resultRelInfo->ri_RelationDesc;
    let relation = estate
        .result_rel(result_rel_info)
        .ri_RelationDesc
        .as_ref()
        .expect("ExecOnConflictUpdate: result relation must be open")
        .alias();
    // TupleTableSlot *existing = resultRelInfo->ri_onConflict->oc_Existing;
    let existing = oc_existing_slot::call(estate, result_rel_info);

    // Parse analysis should have blocked ON CONFLICT for all system relations.
    // Assert(!resultRelInfo->ri_needLockTagTuple);
    debug_assert!(!ri_need_lock_tag_tuple::call(estate, result_rel_info));

    // Determine lock mode to use
    // lockmode = ExecUpdateLockMode(context->estate, resultRelInfo);
    let lockmode: LockTupleMode =
        execMain_seams::exec_update_lock_mode::call(estate, result_rel_info)?;

    // Lock tuple for update.  Don't follow updates when tuple cannot be locked
    // without doing so.  A row locking conflict here means our previous
    // conclusion that the tuple is conclusively committed is not true anymore.
    //
    // test = table_tuple_lock(relation, conflictTid, estate->es_snapshot,
    //          existing, estate->es_output_cid, lockmode, LockWaitBlock, 0, &tmfd);
    let snapshot = es_snapshot::call(estate);
    let cid = estate.es_output_cid;
    let mut tmfd = TM_FailureData::default();
    let test = table_tuple_lock::call(
        estate,
        result_rel_info,
        conflict_tid,
        snapshot,
        existing,
        cid,
        lockmode,
        &mut tmfd,
    )?;

    match test {
        // success!
        TM_Result::TM_Ok => {}

        TM_Result::TM_Invisible => {
            // This can occur when a just inserted tuple is updated again in
            // the same command.  Somewhat similar to the ExecUpdate()
            // TM_SelfModified case.
            //
            // xminDatum = slot_getsysattr(existing,
            //   MinTransactionIdAttributeNumber, &isnull);
            // Assert(!isnull);
            // xmin = DatumGetTransactionId(xminDatum);
            let _ = MinTransactionIdAttributeNumber;
            let (xmin, isnull) = slot_get_xmin::call(estate, existing)?;
            debug_assert!(!isnull);

            if transam_xact_seams::transaction_id_is_current_transaction_id::call(
                xmin,
            ) {
                return Err(PgError::error(
                    "ON CONFLICT DO UPDATE command cannot affect row a second time",
                )
                .with_sqlstate(ERRCODE_CARDINALITY_VIOLATION)
                .with_hint(
                    "Ensure that no rows proposed for insertion within the same command have duplicate constrained values.",
                ));
            }

            // This shouldn't happen
            // elog(ERROR, "attempted to lock invisible tuple");
            return Err(PgError::error("attempted to lock invisible tuple"));
        }

        TM_Result::TM_SelfModified => {
            // This state should never be reached. As a dirty snapshot is used
            // to find conflicting tuples, speculative insertion wouldn't have
            // seen this row to conflict with.
            // elog(ERROR, "unexpected self-updated tuple");
            return Err(PgError::error("unexpected self-updated tuple"));
        }

        TM_Result::TM_Updated => {
            if isolation_uses_xact_snapshot::call() {
                return Err(PgError::error(
                    "could not serialize access due to concurrent update",
                )
                .with_sqlstate(ERRCODE_T_R_SERIALIZATION_FAILURE));
            }

            // Tell caller to try again from the very start.  It does not make
            // sense to use the usual EvalPlanQual() style loop here, as the new
            // version of the row might not conflict anymore, or the conflicting
            // tuple has actually been deleted.
            exec_clear_tuple::call(estate, existing)?;
            return Ok(false);
        }

        TM_Result::TM_Deleted => {
            if isolation_uses_xact_snapshot::call() {
                return Err(PgError::error(
                    "could not serialize access due to concurrent delete",
                )
                .with_sqlstate(ERRCODE_T_R_SERIALIZATION_FAILURE));
            }

            // see TM_Updated case
            exec_clear_tuple::call(estate, existing)?;
            return Ok(false);
        }

        other => {
            // elog(ERROR, "unrecognized table_tuple_lock status: %u", test);
            return Err(PgError::error(format!(
                "unrecognized table_tuple_lock status: {other:?}"
            )));
        }
    }

    // Success, the tuple is locked.

    // Verify that the tuple is visible to our MVCC snapshot if the current
    // isolation level mandates that.
    ExecCheckTupleVisible(estate, relation.alias(), existing)?;

    // Make tuple and any needed join variables available to ExecQual and
    // ExecProject.  The EXCLUDED tuple is installed in ecxt_innertuple, while
    // the target's existing tuple is installed in the scantuple.
    //
    // econtext->ecxt_scantuple = existing;
    // econtext->ecxt_innertuple = excludedSlot;
    // econtext->ecxt_outertuple = NULL;
    oc_set_econtext_tuples::call(estate, mtstate, existing, excluded_slot);

    // if (!ExecQual(onConflictSetWhere, econtext))
    if !exec_qual_oc_where::call(estate, mtstate, result_rel_info)? {
        // ExecClearTuple(existing); see return below
        exec_clear_tuple::call(estate, existing)?;
        // InstrCountFiltered1(&mtstate->ps, 1);
        instr_count_filtered1::call(mtstate, 1);
        // return true; done with the tuple
        return Ok(true);
    }

    // if (resultRelInfo->ri_WithCheckOptions != NIL)
    if ri_has_with_check_options::call(estate, result_rel_info) {
        // Check target's existing tuple against UPDATE-applicable USING
        // security barrier quals, enforced here as RLS checks/WCOs.
        // ExecWithCheckOptions(WCO_RLS_CONFLICT_CHECK, resultRelInfo, existing, mtstate->ps.state);
        exec_with_check_options::call(estate, WCO_RLS_CONFLICT_CHECK, result_rel_info, existing)?;
    }

    // Project the new tuple version
    // ExecProject(resultRelInfo->ri_onConflict->oc_ProjInfo);
    exec_project_oc::call(estate, result_rel_info)?;

    // Note that it is possible that the target tuple has been modified in this
    // session, after the above table_tuple_lock.  We choose to not error out in
    // that case, in line with ExecUpdate's treatment of similar cases.

    // Execute UPDATE with projection
    // *returning = ExecUpdate(context, resultRelInfo, conflictTid, NULL,
    //              existing, resultRelInfo->ri_onConflict->oc_ProjSlot, canSetTag);
    let proj_slot = oc_proj_slot::call(estate, result_rel_info);
    // ExecUpdate takes `tupleid` as an in/out pointer (C mutates `*tupleid`).
    // ON CONFLICT targets never set ri_needLockTagTuple, so the caller does not
    // depend on the post-call advance; thread a local copy.
    let mut oc_tid = *conflict_tid;
    *returning = ExecUpdate(
        mcx,
        context,
        mtstate,
        estate,
        result_rel_info,
        Some(&mut oc_tid),
        None,
        Some(existing),
        proj_slot,
        can_set_tag,
    )?;

    // Clear out existing tuple, as there might not be another conflict among
    // the next input rows.  First though, make sure that the returning slot, if
    // any, has a local copy of any OLD pass-by-reference values, if it refers to
    // any OLD columns.
    //
    // if (*returning != NULL && resultRelInfo->ri_projectReturning->pi_state.flags & EEO_FLAG_HAS_OLD)
    //     ExecMaterializeSlot(*returning);
    if let Some(ret) = *returning {
        if ri_returning_has_old::call(estate, result_rel_info) {
            exec_materialize_slot::call(estate, ret)?;
        }
    }

    // ExecClearTuple(existing);
    exec_clear_tuple::call(estate, existing)?;

    // return true;
    Ok(true)
}

/// `ExecCheckTIDVisible(estate, relinfo, tid, tempSlot)` — under a serializable
/// snapshot, fetch `tid` into `tempSlot` and verify it is visible, raising the
/// ON CONFLICT serialization-failure error otherwise.
pub fn ExecCheckTIDVisible<'mcx>(
    estate: &mut EStateData<'mcx>,
    relinfo: RriId,
    tid: &ItemPointerData,
    temp_slot: SlotId,
) -> PgResult<()> {
    // Relation rel = relinfo->ri_RelationDesc;
    let rel = estate
        .result_rel(relinfo)
        .ri_RelationDesc
        .as_ref()
        .expect("ExecCheckTIDVisible: result relation must be open")
        .alias();

    // Redundantly check isolation level
    // if (!IsolationUsesXactSnapshot()) return;
    if !isolation_uses_xact_snapshot::call() {
        return Ok(());
    }

    // if (!table_tuple_fetch_row_version(rel, tid, SnapshotAny, tempSlot))
    //     elog(ERROR, "failed to fetch conflicting tuple for ON CONFLICT");
    if !table_tuple_fetch_row_version_any::call(estate, relinfo, tid, temp_slot)? {
        return Err(PgError::error(
            "failed to fetch conflicting tuple for ON CONFLICT",
        ));
    }

    // ExecCheckTupleVisible(estate, rel, tempSlot);
    ExecCheckTupleVisible(estate, rel, temp_slot)?;

    // ExecClearTuple(tempSlot);
    exec_clear_tuple::call(estate, temp_slot)?;

    Ok(())
}

/// `ExecCheckTupleVisible(estate, rel, slot)` — under a serializable snapshot,
/// verify `slot`'s tuple is visible to the transaction snapshot, raising a
/// serialization failure otherwise.
pub fn ExecCheckTupleVisible<'mcx>(
    estate: &mut EStateData<'mcx>,
    rel: Relation<'mcx>,
    slot: SlotId,
) -> PgResult<()> {
    // if (!IsolationUsesXactSnapshot()) return;
    if !isolation_uses_xact_snapshot::call() {
        return Ok(());
    }

    // if (!table_tuple_satisfies_snapshot(rel, slot, estate->es_snapshot))
    let snapshot = es_snapshot::call(estate);
    if !table_tuple_satisfies_snapshot::call(estate, rel, slot, snapshot)? {
        // xminDatum = slot_getsysattr(slot, MinTransactionIdAttributeNumber, &isnull);
        // Assert(!isnull);
        // xmin = DatumGetTransactionId(xminDatum);
        let (xmin, isnull) = slot_get_xmin::call(estate, slot)?;
        debug_assert!(!isnull);

        // We should not raise a serialization failure if the conflict is
        // against a tuple inserted by our own transaction, even if it's not
        // visible to our snapshot.
        // if (!TransactionIdIsCurrentTransactionId(xmin))
        if !transam_xact_seams::transaction_id_is_current_transaction_id::call(xmin)
        {
            return Err(PgError::error(
                "could not serialize access due to concurrent update",
            )
            .with_sqlstate(ERRCODE_T_R_SERIALIZATION_FAILURE));
        }
    }

    Ok(())
}
