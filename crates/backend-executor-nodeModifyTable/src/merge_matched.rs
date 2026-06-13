//! The MERGE matched-action dispatch (`ExecMergeMatched`), split out of the
//! `merge` family because the C function (~535 lines) is large enough to
//! body-port independently of the rest of the MERGE path.

use mcx::Mcx;
use types_core::primitive::{BlockNumber, InvalidBlockNumber, OffsetNumber};
use types_error::{
    PgError, PgResult, ERRCODE_CARDINALITY_VIOLATION, ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION,
    ERRCODE_T_R_SERIALIZATION_FAILURE,
};
use types_nodes::nodes::CmdType;
use types_nodes::modifytable::{MergeMatchKind, MERGE_WHEN_MATCHED, MERGE_WHEN_NOT_MATCHED_BY_SOURCE,
    MERGE_WHEN_NOT_MATCHED_BY_TARGET};
use types_nodes::{EStateData, ModifyTableState, RriId, SlotId};
use types_tableam::tableam::{
    LockWaitPolicy, Snapshot, TM_Result, TUPLE_LOCK_FLAG_FIND_LAST_VERSION,
};
use types_snapshot::{SnapshotData, SnapshotType};
use types_tuple::heaptuple::{HeapTuple, ItemPointerData};

use crate::lifecycle::ExecProcessReturning;
use crate::{ModifyTableContext, UpdateContext};

/// `WCO_RLS_MERGE_UPDATE_CHECK` (parsenodes.h `WCOKind`). Values verified
/// against `nodes/parsenodes.h`: the `WCOKind` enumerators are
/// `WCO_VIEW_CHECK=0`, `WCO_RLS_INSERT_CHECK=1`, `WCO_RLS_UPDATE_CHECK=2`,
/// `WCO_RLS_CONFLICT_CHECK=3`, `WCO_RLS_MERGE_UPDATE_CHECK=4`,
/// `WCO_RLS_MERGE_DELETE_CHECK=5`.
const WCO_RLS_MERGE_UPDATE_CHECK: i32 = 4;
/// `WCO_RLS_MERGE_DELETE_CHECK` (parsenodes.h `WCOKind`).
const WCO_RLS_MERGE_DELETE_CHECK: i32 = 5;

/// `InplaceUpdateTupleLock` == `ExclusiveLock` (lockdefs.h). `LOCKMODE` is
/// `i32`.
const INPLACE_UPDATE_TUPLE_LOCK: i32 = 7;

/// `MovedPartitionsBlockNumber` (`storage/itemptr.h`) — `InvalidBlockNumber-1`.
const MOVED_PARTITIONS_BLOCK_NUMBER: BlockNumber = InvalidBlockNumber - 1;
/// `MovedPartitionsOffsetNumber` (`storage/itemptr.h`) — `0xfffd`.
const MOVED_PARTITIONS_OFFSET_NUMBER: OffsetNumber = 0xfffd;

/// `SnapshotAny` (snapmgr) — the static "any tuple visible" snapshot.
fn snapshot_any() -> Snapshot {
    Some(SnapshotData::sentinel(SnapshotType::SNAPSHOT_ANY))
}

/// `ItemPointerIndicatesMovedPartitions(pointer)` (storage/itemptr.h): true iff
/// the TID is the "moved to another partition" sentinel.
fn item_pointer_indicates_moved_partitions(tid: &ItemPointerData) -> bool {
    tid.ip_posid == MOVED_PARTITIONS_OFFSET_NUMBER
        && tid.ip_blkid.block_number() == MOVED_PARTITIONS_BLOCK_NUMBER
}

// ---------------------------------------------------------------------------
// Seams into unported neighbors that `ExecMergeMatched` reaches. The per-action
// MERGE exec state (`ResultRelInfo.ri_MergeActions[]`,
// `ri_MergeJoinCondition`, `ri_RowIdAttNo`, `ri_projectReturning`,
// `ri_WithCheckOptions`) is owned by execMain and has not been published into
// the trimmed shared `types_nodes::ResultRelInfo` vocabulary; the action-loop
// primitives that read or evaluate it therefore go through these owner seams
// (the delete_exec.rs precedent — a unit declares its own marshal+delegate
// slots for not-yet-owned neighbor capabilities, and a call panics with the
// seam path until the owner installs it). They are thin marshal+delegate
// slots: no logic lives in the declaration.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `resultRelInfo->ri_MergeActions[MERGE_WHEN_MATCHED] == NIL &&
    /// resultRelInfo->ri_MergeActions[MERGE_WHEN_NOT_MATCHED_BY_SOURCE] == NIL`
    /// (execnodes.h): are both the WHEN MATCHED and WHEN NOT MATCHED BY SOURCE
    /// action lists empty for this result relation?
    pub fn ri_merge_matched_actions_empty(
        estate: &EStateData<'_>,
        result_rel_info: RriId
    ) -> bool
);

seam_core::seam!(
    /// `list_length(resultRelInfo->ri_MergeActions[kind])` (execnodes.h): number
    /// of MERGE action states of the given match kind for this result relation.
    pub fn ri_merge_actions_count(
        estate: &EStateData<'_>,
        result_rel_info: RriId,
        kind: MergeMatchKind
    ) -> i32
);

seam_core::seam!(
    /// `((MergeActionState *) list_nth(ri_MergeActions[kind], idx))
    /// ->mas_action->commandType` (execnodes.h / primnodes.h): the command type
    /// of the idx-th MERGE action of the given kind.
    pub fn ri_merge_action_command_type(
        estate: &EStateData<'_>,
        result_rel_info: RriId,
        kind: MergeMatchKind,
        idx: i32
    ) -> CmdType
);

seam_core::seam!(
    /// `((MergeActionState *) list_nth(ri_MergeActions[kind], idx))
    /// ->mas_action->matchKind` (primnodes.h): the match kind recorded on the
    /// idx-th MERGE action's underlying `MergeAction` (used to decide whether a
    /// concurrent update can switch from MATCHED to NOT MATCHED BY SOURCE).
    pub fn ri_merge_action_match_kind(
        estate: &EStateData<'_>,
        result_rel_info: RriId,
        kind: MergeMatchKind,
        idx: i32
    ) -> MergeMatchKind
);

seam_core::seam!(
    /// `mtstate->mt_merge_action = (MergeActionState *)
    /// list_nth(ri_MergeActions[kind], idx)` (nodeModifyTable.c): record the
    /// idx-th MERGE action of the given kind as the current action on the
    /// `ModifyTableState`. The action state lives on the unported
    /// `ResultRelInfo`, so the owner does the assignment by id.
    pub fn ri_set_current_merge_action<'mcx>(
        estate: &mut EStateData<'mcx>,
        mtstate: &mut ModifyTableState<'mcx>,
        result_rel_info: RriId,
        kind: MergeMatchKind,
        idx: i32
    )
);

seam_core::seam!(
    /// `ExecQual(relaction->mas_whenqual, econtext)` (executor.h): evaluate the
    /// WHEN [NOT MATCHED] AND conditions of the idx-th MERGE action of the given
    /// kind over the node's expression context. A `NULL` whenqual is
    /// always-true. Fallible on `ereport(ERROR)`.
    pub fn ri_merge_action_eval_whenqual<'mcx>(
        estate: &mut EStateData<'mcx>,
        result_rel_info: RriId,
        kind: MergeMatchKind,
        idx: i32,
        econtext: types_nodes::EcxtId
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `ExecProject(relaction->mas_proj)` (executor.h): project the idx-th MERGE
    /// action's targetlist into its output slot, returning the slot id. Fallible
    /// on `ereport(ERROR)` from a projection expression.
    pub fn ri_merge_action_project<'mcx>(
        estate: &mut EStateData<'mcx>,
        result_rel_info: RriId,
        kind: MergeMatchKind,
        idx: i32
    ) -> PgResult<SlotId>
);

seam_core::seam!(
    /// `ExecQual(resultRelInfo->ri_MergeJoinCondition, econtext)` (executor.h):
    /// evaluate this result relation's MERGE join condition over the node's
    /// expression context (NULL condition is always-true — see
    /// transform_MERGE_to_join). Fallible on `ereport(ERROR)`.
    pub fn ri_eval_merge_join_condition<'mcx>(
        estate: &mut EStateData<'mcx>,
        result_rel_info: RriId,
        econtext: types_nodes::EcxtId
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `resultRelInfo->ri_WithCheckOptions != NIL` (execnodes.h): does this
    /// result relation carry WITH CHECK OPTION / RLS policies?
    pub fn ri_has_with_check_options(estate: &EStateData<'_>, result_rel_info: RriId) -> bool
);

seam_core::seam!(
    /// `resultRelInfo->ri_projectReturning != NULL` (execnodes.h): does this
    /// result relation carry a RETURNING projection?
    pub fn ri_has_project_returning(estate: &EStateData<'_>, result_rel_info: RriId) -> bool
);

seam_core::seam!(
    /// `(void) ExecGetJunkAttribute(slot, attno, &isNull)` (execJunk.c): fetch
    /// the junk attribute `attno` from `slot`, returning whether it is NULL
    /// (the MERGE recheck only needs the is-null flag of the row-id junk attr).
    /// Fallible on `ereport(ERROR)`.
    pub fn exec_get_junk_attribute_isnull<'mcx>(
        estate: &mut EStateData<'mcx>,
        slot: SlotId,
        attno: i32
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `resultRelInfo->ri_RowIdAttNo` (execnodes.h): the junk attribute number
    /// carrying the target row's identity (ctid / wholerow), used to detect a
    /// no-longer-matching subplan row during the MERGE EPQ recheck.
    pub fn ri_row_id_att_no(estate: &EStateData<'_>, result_rel_info: RriId) -> i32
);

seam_core::seam!(
    /// `ExecForceStoreHeapTuple(tuple, slot, shouldFree)` (execTuples.c): force
    /// the given heap tuple into the slot.
    pub fn exec_force_store_heap_tuple<'mcx>(
        estate: &mut EStateData<'mcx>,
        tuple: HeapTuple<'mcx>,
        slot: SlotId,
        should_free: bool
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `outerPlanState(mtstate)->instrument != NULL` (execnodes.h): is the outer
    /// (subplan) node of this ModifyTable being instrumented?
    pub fn outer_plan_state_instrumented(mtstate: &ModifyTableState<'_>) -> bool
);

seam_core::seam!(
    /// `InstrUpdateTupleCount(outerPlanState(mtstate)->instrument, ntuples)`
    /// (executor/instrument.c): bump the outer node's tuple count by `ntuples`,
    /// used to keep the "skipped" row count correct when a concurrently-updated
    /// MATCHED row is reclassified as not-matched.
    pub fn outer_plan_state_instr_update_tuple_count<'mcx>(
        mtstate: &mut ModifyTableState<'mcx>,
        ntuples: f64
    )
);

/// `ExecMergeMatched(context, resultRelInfo, tupleid, oldtuple, canSetTag,
/// matched)` — run the WHEN MATCHED / NOT MATCHED BY SOURCE actions in order,
/// performing the chosen UPDATE/DELETE/DO NOTHING with concurrent-update
/// rechecks. Sets `matched = false` to signal a NOT MATCHED BY TARGET retry.
/// Returns the RETURNING slot or `None`.
pub fn ExecMergeMatched<'mcx>(
    mcx: Mcx<'mcx>,
    context: &mut ModifyTableContext,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    tupleid: Option<&ItemPointerData>,
    oldtuple: HeapTuple<'mcx>,
    can_set_tag: bool,
    matched: &mut bool,
) -> PgResult<Option<SlotId>> {
    let mut newslot: Option<SlotId> = None;
    let mut rslot: Option<SlotId> = None;

    // ExprContext *econtext = mtstate->ps.ps_ExprContext;
    let econtext = mtstate
        .ps
        .ps_ExprContext
        .expect("MERGE node has an expression context");

    // EPQState *epqstate = &mtstate->mt_epqstate; (threaded explicitly below)

    // Expect matched to be true on entry.
    debug_assert!(*matched);

    // If there are no WHEN MATCHED or WHEN NOT MATCHED BY SOURCE actions, we
    // are done.
    if ri_merge_matched_actions_empty::call(estate, result_rel_info) {
        return Ok(None);
    }

    // Make tuple and any needed join variables available to ExecQual and
    // ExecProject. The target's existing tuple is installed in the scantuple.
    //
    //   econtext->ecxt_scantuple = resultRelInfo->ri_oldTupleSlot;
    //   econtext->ecxt_innertuple = context->planSlot;
    //   econtext->ecxt_outertuple = NULL;
    let old_tuple_slot = estate.result_rel(result_rel_info).ri_oldTupleSlot;
    let plan_slot = context.planSlot;
    {
        let ecxt = estate.ecxt_mut(econtext);
        ecxt.ecxt_scantuple = old_tuple_slot;
        ecxt.ecxt_innertuple = plan_slot;
        ecxt.ecxt_outertuple = None;
    }

    // This routine is only invoked for matched target rows, so we should either
    // have the tupleid of the target row, or an old tuple from the target
    // wholerow junk attr.
    debug_assert!(tupleid.is_some() || oldtuple.is_some());

    // ItemPointerSetInvalid(&lockedtid); modeled as Option<ItemPointerData>
    // (the C sentinel + ItemPointerIsValid test).
    let mut lockedtid: Option<ItemPointerData> = None;

    let need_lock_tag_tuple = estate.result_rel(result_rel_info).ri_needLockTagTuple;
    let result_relation_oid = relation_oid(estate, result_rel_info);
    let old_tuple_slot =
        old_tuple_slot.expect("MERGE result relation has an initialized ri_oldTupleSlot");

    if let Some(tuple) = oldtuple.clone() {
        debug_assert!(!need_lock_tag_tuple);
        exec_force_store_heap_tuple::call(estate, Some(tuple), old_tuple_slot, false)?;
    } else {
        let tid = *tupleid.expect("MERGE matched row without tupleid must have oldtuple");
        if need_lock_tag_tuple {
            // This locks even for CMD_DELETE, for CMD_NOTHING, and for tuples
            // that don't match mas_whenqual.  MERGE on system catalogs is a
            // minor use case, so don't bother optimizing those.
            backend_storage_lmgr_lmgr_seams::lock_tuple::call(
                result_relation_oid,
                tid,
                INPLACE_UPDATE_TUPLE_LOCK,
            )?;
            lockedtid = Some(tid);
        }
        let rel = relation_alias(estate, result_rel_info);
        let any = snapshot_any();
        let slot_ref = estate.slot_mut(old_tuple_slot);
        if !backend_access_table_tableam::table_tuple_fetch_row_version(
            &rel, &tid, &any, slot_ref,
        )? {
            return finish(estate, result_rel_info, lockedtid, None);
        }
    }

    // Test the join condition.  If it's satisfied, perform a MATCHED action.
    // Otherwise, perform a NOT MATCHED BY SOURCE action.
    //
    // Note that this join condition will be NULL if there are no NOT MATCHED BY
    // SOURCE actions --- see transform_MERGE_to_join(). In that case, we need
    // only consider MATCHED actions here.
    let mut action_kind = if ri_eval_merge_join_condition::call(estate, result_rel_info, econtext)?
    {
        MERGE_WHEN_MATCHED
    } else {
        MERGE_WHEN_NOT_MATCHED_BY_SOURCE
    };

    // lmerge_matched: loop label; the C "goto lmerge_matched" restarts the
    // action search after a concurrent update.
    'lmerge_matched: loop {
        let n_actions = ri_merge_actions_count::call(estate, result_rel_info, action_kind);

        let mut idx = 0;
        while idx < n_actions {
            let command_type =
                ri_merge_action_command_type::call(estate, result_rel_info, action_kind, idx);
            let result: TM_Result;
            let mut update_cxt = UpdateContext {
                crossPartUpdate: false,
                updateIndexes: types_tableam::tableam::TU_UpdateIndexes::TU_None,
                // C: zero-initialized stack field, overwritten by ExecUpdateAct.
                lockmode: types_tableam::tableam::LockTupleMode::LockTupleKeyShare,
            };

            // Test condition, if any. In the absence of any condition, we
            // perform the action unconditionally (ExecQual returns true if
            // there are no conditions to evaluate).
            if !ri_merge_action_eval_whenqual::call(
                estate,
                result_rel_info,
                action_kind,
                idx,
                econtext,
            )? {
                idx += 1;
                continue;
            }

            // Check if the existing target tuple meets the USING checks of
            // UPDATE/DELETE RLS policies. If those checks fail, we throw an
            // error. The WITH CHECK quals for UPDATE RLS policies are applied
            // in ExecUpdateAct(). We must do this after WHEN quals are
            // evaluated, so that we check policies only when they matter.
            if ri_has_with_check_options::call(estate, result_rel_info)
                && command_type != CmdType::CMD_NOTHING
            {
                let kind = if command_type == CmdType::CMD_UPDATE {
                    WCO_RLS_MERGE_UPDATE_CHECK
                } else {
                    WCO_RLS_MERGE_DELETE_CHECK
                };
                backend_executor_execMain_seams::exec_with_check_options::call(
                    estate,
                    kind,
                    result_rel_info,
                    old_tuple_slot,
                )?;
            }

            // Perform stated action.
            match command_type {
                CmdType::CMD_UPDATE => {
                    // Project the output tuple, and use that to update the
                    // table.  We don't need to filter out junk attributes,
                    // because the UPDATE action's targetlist doesn't have any.
                    let projected =
                        ri_merge_action_project::call(estate, result_rel_info, action_kind, idx)?;
                    newslot = Some(projected);

                    ri_set_current_merge_action::call(
                        estate,
                        mtstate,
                        result_rel_info,
                        action_kind,
                        idx,
                    );

                    let mut prologue_result = TM_Result::TM_Ok;
                    if !crate::update::ExecUpdatePrologue(
                        mcx,
                        context,
                        mtstate,
                        estate,
                        result_rel_info,
                        tupleid,
                        None,
                        projected,
                        Some(&mut prologue_result),
                    )? {
                        if prologue_result == TM_Result::TM_Ok {
                            // "do nothing"
                            return finish(estate, result_rel_info, lockedtid, rslot);
                        }
                        // concurrent update/delete
                        break;
                    }

                    // INSTEAD OF ROW UPDATE Triggers
                    if estate.result_rel(result_rel_info).ri_trig_update_instead_row {
                        if !backend_commands_trigger_seams::exec_ir_update_triggers::call(
                            estate,
                            result_rel_info,
                            oldtuple.clone(),
                            projected,
                        )? {
                            // "do nothing"
                            return finish(estate, result_rel_info, lockedtid, rslot);
                        }
                        result = TM_Result::TM_Ok;
                    } else {
                        // checked ri_needLockTagTuple above
                        debug_assert!(oldtuple.is_none());

                        result = crate::update::ExecUpdateAct(
                            mcx,
                            context,
                            mtstate,
                            estate,
                            result_rel_info,
                            tupleid,
                            None,
                            projected,
                            can_set_tag,
                            &mut update_cxt,
                        )?;

                        // As in ExecUpdate(), if ExecUpdateAct() reports that a
                        // cross-partition update was done, then there's nothing
                        // else for us to do --- the UPDATE has been turned into
                        // a DELETE and an INSERT, and the RETURNING tuple (if
                        // any) has been projected, so we can just return that.
                        if update_cxt.crossPartUpdate {
                            mtstate.mt_merge_updated += 1.0;
                            rslot = context.cpUpdateReturningSlot;
                            return finish(estate, result_rel_info, lockedtid, rslot);
                        }
                    }

                    if result == TM_Result::TM_Ok {
                        crate::update::ExecUpdateEpilogue(
                            mcx,
                            context,
                            mtstate,
                            estate,
                            &update_cxt,
                            result_rel_info,
                            tupleid,
                            None,
                            projected,
                        )?;
                        mtstate.mt_merge_updated += 1.0;
                    }
                }

                CmdType::CMD_DELETE => {
                    ri_set_current_merge_action::call(
                        estate,
                        mtstate,
                        result_rel_info,
                        action_kind,
                        idx,
                    );

                    let mut prologue_result = TM_Result::TM_Ok;
                    if !crate::delete::ExecDeletePrologue(
                        mcx,
                        context,
                        mtstate,
                        estate,
                        result_rel_info,
                        tupleid,
                        None,
                        None,
                        Some(&mut prologue_result),
                    )? {
                        if prologue_result == TM_Result::TM_Ok {
                            // "do nothing"
                            return finish(estate, result_rel_info, lockedtid, rslot);
                        }
                        // concurrent update/delete
                        break;
                    }

                    // INSTEAD OF ROW DELETE Triggers
                    if crate::delete_exec::ri_has_instead_delete_row::call(estate, result_rel_info)
                    {
                        if !crate::delete_exec::exec_ir_delete_triggers::call(
                            estate,
                            result_rel_info,
                            oldtuple.clone(),
                        )? {
                            // "do nothing"
                            return finish(estate, result_rel_info, lockedtid, rslot);
                        }
                        result = TM_Result::TM_Ok;
                    } else {
                        // checked ri_needLockTagTuple above
                        debug_assert!(oldtuple.is_none());

                        let tid = *tupleid
                            .expect("MERGE CMD_DELETE on a table requires a target tupleid");
                        result = crate::delete::ExecDeleteAct(
                            context,
                            estate,
                            result_rel_info,
                            &tid,
                            false,
                        )?;
                    }

                    if result == TM_Result::TM_Ok {
                        crate::delete::ExecDeleteEpilogue(
                            mcx,
                            context,
                            mtstate,
                            estate,
                            result_rel_info,
                            tupleid,
                            None,
                            false,
                        )?;
                        mtstate.mt_merge_deleted += 1.0;
                    }
                }

                CmdType::CMD_NOTHING => {
                    // Doing nothing is always OK.
                    result = TM_Result::TM_Ok;
                }

                _ => {
                    return Err(PgError::error("unknown action in MERGE WHEN clause"));
                }
            }

            match result {
                TM_Result::TM_Ok => {
                    // all good; perform final actions
                    if can_set_tag && command_type != CmdType::CMD_NOTHING {
                        estate.es_processed += 1;
                    }
                }

                TM_Result::TM_SelfModified => {
                    // The target tuple was already updated or deleted by the
                    // current command, or by a later command in the current
                    // transaction. The former case is explicitly disallowed by
                    // the SQL standard for MERGE. The latter arises from a
                    // BEFORE-trigger or volatile-function command; throwing an
                    // error is the only safe course.
                    if context.tmfd.cmax != estate.es_output_cid {
                        return Err(PgError::error(
                            "tuple to be updated or deleted was already modified by an operation triggered by the current command",
                        )
                        .with_sqlstate(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION)
                        .with_hint(
                            "Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.",
                        ));
                    }

                    if backend_access_transam_xact_seams::transaction_id_is_current_transaction_id::call(
                        context.tmfd.xmax,
                    ) {
                        return Err(PgError::error("MERGE command cannot affect row a second time")
                            .with_sqlstate(ERRCODE_CARDINALITY_VIOLATION)
                            .with_hint(
                                "Ensure that not more than one source row matches any one target row.",
                            ));
                    }

                    // This shouldn't happen.
                    return Err(PgError::error(
                        "attempted to update or delete invisible tuple",
                    ));
                }

                TM_Result::TM_Deleted => {
                    if backend_access_transam_xact_seams::isolation_uses_xact_snapshot::call() {
                        return Err(PgError::error(
                            "could not serialize access due to concurrent delete",
                        )
                        .with_sqlstate(ERRCODE_T_R_SERIALIZATION_FAILURE));
                    }
                    // If the tuple was already deleted, set matched to false to
                    // let caller handle it under NOT MATCHED [BY TARGET] clauses.
                    *matched = false;
                    return finish(estate, result_rel_info, lockedtid, rslot);
                }

                TM_Result::TM_Updated => {
                    // The target tuple was concurrently updated by some other
                    // transaction. If we are currently processing a MATCHED
                    // action, use EvalPlanQual() with the new version of the
                    // tuple and recheck the join qual, to detect a change from
                    // the MATCHED to the NOT MATCHED cases. If we are already
                    // processing a NOT MATCHED BY SOURCE action, we skip this.
                    let was_matched = ri_merge_action_match_kind::call(
                        estate,
                        result_rel_info,
                        action_kind,
                        idx,
                    ) == MERGE_WHEN_MATCHED;
                    let rti = estate.result_rel(result_rel_info).ri_RangeTableIndex;
                    let lockmode = backend_executor_execMain_seams::exec_update_lock_mode::call(
                        estate,
                        result_rel_info,
                    )?;

                    let inputslot: SlotId = if was_matched {
                        backend_executor_execMain_seams::eval_plan_qual_slot::call(
                            estate,
                            &mut mtstate.mt_epqstate,
                            result_rel_info,
                            rti,
                        )?
                    } else {
                        old_tuple_slot
                    };

                    let tid = *tupleid
                        .expect("MERGE concurrent-update recheck requires a target tupleid");
                    let rel = relation_alias(estate, result_rel_info);
                    let snapshot = estate.es_snapshot.as_deref().cloned();
                    let cid = estate.es_output_cid;
                    let inslot = estate.slot_mut(inputslot);
                    let lock_result = backend_access_table_tableam::table_tuple_lock(
                        &rel,
                        &tid,
                        &snapshot,
                        inslot,
                        cid,
                        lockmode,
                        LockWaitPolicy::LockWaitBlock,
                        TUPLE_LOCK_FLAG_FIND_LAST_VERSION,
                        &mut context.tmfd,
                    )?;

                    match lock_result {
                        TM_Result::TM_Ok => {
                            // If the tuple was updated and migrated to another
                            // partition concurrently, the current MERGE
                            // implementation can't follow.
                            if item_pointer_indicates_moved_partitions(&tid) {
                                return Err(PgError::error(
                                    "tuple to be merged was already moved to another partition due to concurrent update",
                                )
                                .with_sqlstate(ERRCODE_T_R_SERIALIZATION_FAILURE));
                            }

                            // If this was a MATCHED case, use EvalPlanQual() to
                            // recheck the join condition.
                            if was_matched {
                                let epqslot =
                                    backend_executor_execMain_seams::eval_plan_qual::call(
                                        estate,
                                        &mut mtstate.mt_epqstate,
                                        result_rel_info,
                                        rti,
                                        inputslot,
                                    )?;

                                // If the subplan didn't return a tuple, then we
                                // must be dealing with an inner join for which
                                // the join condition no longer matches. There is
                                // nothing more to do.
                                let epqslot = match epqslot {
                                    None => {
                                        return finish(estate, result_rel_info, lockedtid, rslot)
                                    }
                                    Some(s) => s,
                                };

                                // If we got a NULL ctid from the subplan, the
                                // join quals no longer pass and we switch to the
                                // NOT MATCHED BY SOURCE case.
                                let row_id_att =
                                    ri_row_id_att_no::call(estate, result_rel_info);
                                let is_null = exec_get_junk_attribute_isnull::call(
                                    estate, epqslot, row_id_att,
                                )?;
                                if is_null {
                                    *matched = false;
                                }

                                // Otherwise, recheck the join quals to see if we
                                // need to switch to the NOT MATCHED BY SOURCE
                                // case.
                                if estate.result_rel(result_rel_info).ri_needLockTagTuple {
                                    if let Some(locked) = lockedtid {
                                        backend_storage_lmgr_lmgr_seams::unlock_tuple::call(
                                            result_relation_oid,
                                            locked,
                                            INPLACE_UPDATE_TUPLE_LOCK,
                                        )?;
                                    }
                                    backend_storage_lmgr_lmgr_seams::lock_tuple::call(
                                        result_relation_oid,
                                        tid,
                                        INPLACE_UPDATE_TUPLE_LOCK,
                                    )?;
                                    lockedtid = Some(tid);
                                }

                                let rel2 = relation_alias(estate, result_rel_info);
                                let any = snapshot_any();
                                let oldslot_ref = estate.slot_mut(old_tuple_slot);
                                if !backend_access_table_tableam::table_tuple_fetch_row_version(
                                    &rel2, &tid, &any, oldslot_ref,
                                )? {
                                    return Err(PgError::error(
                                        "failed to fetch the target tuple",
                                    ));
                                }

                                if *matched {
                                    *matched = ri_eval_merge_join_condition::call(
                                        estate,
                                        result_rel_info,
                                        econtext,
                                    )?;
                                }

                                // Switch lists, if necessary.
                                if !*matched {
                                    action_kind = MERGE_WHEN_NOT_MATCHED_BY_SOURCE;

                                    // If we have both NOT MATCHED BY SOURCE and
                                    // NOT MATCHED BY TARGET actions (a full join
                                    // between source and target), the single
                                    // previously matched outer tuple is treated
                                    // as two not-matched tuples; adjust the outer
                                    // node's tuple count, if instrumenting, to
                                    // keep the "skipped" row count correct.
                                    if outer_plan_state_instrumented::call(mtstate)
                                        && ri_merge_actions_count::call(
                                            estate,
                                            result_rel_info,
                                            MERGE_WHEN_NOT_MATCHED_BY_SOURCE,
                                        ) > 0
                                        && ri_merge_actions_count::call(
                                            estate,
                                            result_rel_info,
                                            MERGE_WHEN_NOT_MATCHED_BY_TARGET,
                                        ) > 0
                                    {
                                        outer_plan_state_instr_update_tuple_count::call(
                                            mtstate, 1.0,
                                        );
                                    }
                                }
                            }

                            // Loop back and process the MATCHED or NOT MATCHED
                            // BY SOURCE actions from the start.
                            continue 'lmerge_matched;
                        }

                        TM_Result::TM_Deleted => {
                            // tuple already deleted; tell caller to run NOT
                            // MATCHED [BY TARGET] actions
                            *matched = false;
                            return finish(estate, result_rel_info, lockedtid, rslot);
                        }

                        TM_Result::TM_SelfModified => {
                            // Reached when following an update chain from a
                            // tuple updated by another session, reaching a tuple
                            // already updated or deleted by the current command
                            // (or a later command in this transaction). As
                            // above, always an error.
                            if context.tmfd.cmax != estate.es_output_cid {
                                return Err(PgError::error(
                                    "tuple to be updated or deleted was already modified by an operation triggered by the current command",
                                )
                                .with_sqlstate(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION)
                                .with_hint(
                                    "Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.",
                                ));
                            }

                            if backend_access_transam_xact_seams::transaction_id_is_current_transaction_id::call(
                                context.tmfd.xmax,
                            ) {
                                return Err(PgError::error(
                                    "MERGE command cannot affect row a second time",
                                )
                                .with_sqlstate(ERRCODE_CARDINALITY_VIOLATION)
                                .with_hint(
                                    "Ensure that not more than one source row matches any one target row.",
                                ));
                            }

                            // This shouldn't happen.
                            return Err(PgError::error(
                                "attempted to update or delete invisible tuple",
                            ));
                        }

                        other => {
                            // see table_tuple_lock call in ExecDelete()
                            return Err(PgError::error(format!(
                                "unexpected table_tuple_lock status: {}",
                                other as u32
                            )));
                        }
                    }
                }

                TM_Result::TM_Invisible
                | TM_Result::TM_WouldBlock
                | TM_Result::TM_BeingModified => {
                    // these should not occur
                    return Err(PgError::error(format!(
                        "unexpected tuple operation result: {}",
                        result as i32
                    )));
                }
            }

            // Process RETURNING if present.
            if ri_has_project_returning::call(estate, result_rel_info) {
                match command_type {
                    CmdType::CMD_UPDATE => {
                        rslot = Some(ExecProcessReturning(
                            estate,
                            result_rel_info,
                            CmdType::CMD_UPDATE,
                            Some(old_tuple_slot),
                            newslot,
                            context.planSlot,
                        )?);
                    }
                    CmdType::CMD_DELETE => {
                        rslot = Some(ExecProcessReturning(
                            estate,
                            result_rel_info,
                            CmdType::CMD_DELETE,
                            Some(old_tuple_slot),
                            None,
                            context.planSlot,
                        )?);
                    }
                    CmdType::CMD_NOTHING => {}
                    _ => {
                        return Err(PgError::error(format!(
                            "unrecognized commandType: {}",
                            command_type as i32
                        )));
                    }
                }
            }

            // We've activated one of the WHEN clauses, so we don't search
            // further. This is required behaviour, not an optimization.
            break;
        }

        // Successfully executed an action or no qualifying action was found.
        break;
    }

    // out:
    finish(estate, result_rel_info, lockedtid, rslot)
}

/// The C `out:` label epilogue: release the in-place update tuple lock if held
/// and return the RETURNING slot.
fn finish<'mcx>(
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    lockedtid: Option<ItemPointerData>,
    rslot: Option<SlotId>,
) -> PgResult<Option<SlotId>> {
    if let Some(tid) = lockedtid {
        let oid = relation_oid(estate, result_rel_info);
        backend_storage_lmgr_lmgr_seams::unlock_tuple::call(oid, tid, INPLACE_UPDATE_TUPLE_LOCK)?;
    }
    Ok(rslot)
}

/// `RelationGetRelid(resultRelInfo->ri_RelationDesc)`.
fn relation_oid(estate: &EStateData<'_>, rri: RriId) -> types_core::Oid {
    estate
        .result_rel(rri)
        .ri_RelationDesc
        .as_ref()
        .map(|r| r.rd_id)
        .unwrap_or(types_core::INVALID_OID)
}

/// An `alias()` of `ri_RelationDesc` (shared, no release authority).
fn relation_alias<'mcx>(estate: &EStateData<'mcx>, rri: RriId) -> types_rel::Relation<'mcx> {
    estate
        .result_rel(rri)
        .ri_RelationDesc
        .as_ref()
        .expect("ResultRelInfo has no relation")
        .alias()
}
