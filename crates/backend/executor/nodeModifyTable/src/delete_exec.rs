//! The DELETE driver (`ExecDelete`), split out of the `delete` family because
//! the C function (~337 lines) is large enough to body-port independently of
//! the prologue/act/epilogue helpers.

use ::mcx::Mcx;
use ::types_core::xact::CommandId;
use ::types_error::{
    PgError, PgResult, ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION, ERRCODE_T_R_SERIALIZATION_FAILURE,
};
use ::nodes::nodes::CmdType;
use ::nodes::{EStateData, ModifyTableState, RriId, SlotId};
use ::types_tableam::tableam::{
    LockTupleExclusive, LockTupleMode, Snapshot, TM_FailureData, TM_Result,
};
use ::types_tuple::heaptuple::FormedTuple;
use ::types_tuple::heaptuple::ItemPointerData;

use crate::delete::{ExecDeleteAct, ExecDeleteEpilogue, ExecDeletePrologue};
use crate::lifecycle::ExecProcessReturning;
use crate::ModifyTableContext;

/// `LockWaitPolicy` (`nodes/lockoptions.h`) — how `table_tuple_lock` deals
/// with a row already locked by another session. Defined here as the local
/// vocabulary for the `table_tuple_lock` seam this unit owns until the
/// table-AM owner installs the matching declaration. Values verified against
/// `lockoptions.h` (the ordering is significant: highest value wins).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum LockWaitPolicy {
    /// Wait for the lock to become available (default behavior).
    LockWaitBlock = 0,
    /// Skip rows that can't be locked (SKIP LOCKED).
    LockWaitSkip,
    /// Raise an error if a row cannot be locked (NOWAIT).
    LockWaitError,
}

// ---------------------------------------------------------------------------
// Seams into unported neighbors that `ExecDelete` reaches. Each is a thin
// marshal+delegate slot the owner installs when it lands; until then a call
// panics with the seam's path (AGENTS.md: loud panic over a silent stub).
// These mirror the C `ResultRelInfo`/`EState`/slot fields and the
// trigger/EvalPlanQual/table-AM/snapshot routines that the node layer drives
// across a dependency cycle.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `resultRelInfo->ri_TrigDesc && resultRelInfo->ri_TrigDesc->trig_delete_instead_row`
    /// (execnodes.h / reltrigger.h): does this relation have an INSTEAD OF ROW
    /// DELETE trigger? Read off the relcache trigdesc the relation owns.
    pub fn ri_has_instead_delete_row(estate: &EStateData<'_>, result_rel_info: RriId) -> bool
);

seam_core::seam!(
    /// `resultRelInfo->ri_FdwRoutine != NULL` (execnodes.h): is this result
    /// relation a foreign table driven by an FDW?
    pub fn ri_has_fdw_routine(estate: &EStateData<'_>, result_rel_info: RriId) -> bool
);

seam_core::seam!(
    /// `resultRelInfo->ri_projectReturning != NULL` (execnodes.h): does this
    /// result relation carry a RETURNING projection?
    pub fn ri_has_project_returning(estate: &EStateData<'_>, result_rel_info: RriId) -> bool
);

seam_core::seam!(
    /// `resultRelInfo->ri_projectReturning->pi_state.flags & EEO_FLAG_HAS_OLD`
    /// (execnodes.h / execExpr.h): does the RETURNING projection reference any
    /// OLD column values?
    pub fn ri_returning_has_old(estate: &EStateData<'_>, result_rel_info: RriId) -> bool
);

seam_core::seam!(
    /// `RelationGetRelid(resultRelInfo->ri_RelationDesc)` (rel.h): the OID of
    /// this result relation's open descriptor.
    pub fn ri_relation_relid(
        estate: &EStateData<'_>,
        result_rel_info: RriId
    ) -> ::types_core::Oid
);


seam_core::seam!(
    /// `resultRelInfo->ri_FdwRoutine->ExecForeignDelete(estate, relinfo, slot,
    /// planSlot)` (fdwapi.h): let the FDW perform the delete. Returns the slot
    /// the FDW filled, or `None` ("do nothing").
    pub fn fdw_exec_foreign_delete<'mcx>(
        estate: &mut EStateData<'mcx>,
        result_rel_info: RriId,
        slot: SlotId,
        plan_slot: Option<SlotId>,
    ) -> PgResult<Option<SlotId>>
);

seam_core::seam!(
    /// `ExecGetReturningSlot(estate, relinfo)` (execMain.c): get (creating on
    /// first use) the RETURNING slot for this result relation. Allocates in
    /// the per-query context, so fallible on OOM.
    pub fn exec_get_returning_slot<'mcx>(
        estate: &mut EStateData<'mcx>,
        result_rel_info: RriId,
    ) -> PgResult<SlotId>
);

seam_core::seam!(
    /// `TTS_EMPTY(slot)` (tuptable.h): the slot currently holds no tuple.
    pub fn slot_is_empty(estate: &EStateData<'_>, slot: SlotId) -> bool
);

seam_core::seam!(
    /// `ExecStoreAllNullTuple(slot)` (execTuples.c): store an all-NULL virtual
    /// tuple into the slot.
    pub fn exec_store_all_null_tuple<'mcx>(
        estate: &mut EStateData<'mcx>,
        slot: SlotId,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `slot->tts_tableOid = relid` (tuptable.h): set the slot's reported table
    /// OID before evaluating RETURNING (which may reference tableoid).
    pub fn slot_set_table_oid<'mcx>(
        estate: &mut EStateData<'mcx>,
        slot: SlotId,
        relid: ::types_core::Oid,
    )
);

seam_core::seam!(
    /// `IsolationUsesXactSnapshot()` (xact.h): is the current isolation level
    /// REPEATABLE READ or SERIALIZABLE (i.e. transaction-snapshot mode)?
    pub fn isolation_uses_xact_snapshot() -> bool
);

seam_core::seam!(
    /// `EvalPlanQualBegin(epqstate)` (execMain.c): prepare the EPQ machinery
    /// for a fresh recheck of the latest row versions.
    pub fn eval_plan_qual_begin<'mcx>(mtstate: &mut ModifyTableState<'mcx>) -> PgResult<()>
);

seam_core::seam!(
    /// `EvalPlanQualSlot(epqstate, relation, rti)` (execMain.c): get the EPQ
    /// input slot for the given relation/range-table index.
    pub fn eval_plan_qual_slot<'mcx>(
        estate: &mut EStateData<'mcx>,
        mtstate: &mut ModifyTableState<'mcx>,
        result_rel_info: RriId,
        rti: ::types_core::primitive::Index,
    ) -> PgResult<SlotId>
);

seam_core::seam!(
    /// `EvalPlanQual(epqstate, relation, rti, inputslot)` (execMain.c): run the
    /// recheck plan for the locked latest row version; returns the surviving
    /// slot, or `None` (TupIsNull) when the row no longer passes the quals.
    pub fn eval_plan_qual<'mcx>(
        estate: &mut EStateData<'mcx>,
        mtstate: &mut ModifyTableState<'mcx>,
        result_rel_info: RriId,
        inputslot: SlotId,
    ) -> PgResult<Option<SlotId>>
);

seam_core::seam!(
    /// `TupIsNull(slot)` (tuptable.h): the slot is NULL/empty (EPQ "no row").
    pub fn slot_is_null(estate: &EStateData<'_>, slot: SlotId) -> bool
);

seam_core::seam!(
    /// `es_snapshot` (execnodes.h): the EState's active scan snapshot, used by
    /// `table_tuple_lock`. Passed explicitly until execMain owns it.
    pub fn es_snapshot(estate: &EStateData<'_>) -> Snapshot
);

seam_core::seam!(
    /// `table_tuple_lock(rel, tid, snapshot, slot, cid, mode, wait, flags,
    /// tmfd)` (tableam.h): lock the latest version of the tuple into `slot`.
    /// Returns the lock outcome.
    pub fn table_tuple_lock<'mcx>(
        estate: &mut EStateData<'mcx>,
        result_rel_info: RriId,
        tupleid: &ItemPointerData,
        snapshot: Snapshot,
        slot: SlotId,
        cid: CommandId,
        mode: LockTupleMode,
        wait: LockWaitPolicy,
        find_last_version: bool,
        tmfd: &mut TM_FailureData,
    ) -> PgResult<TM_Result>
);

seam_core::seam!(
    /// `table_tuple_fetch_row_version(rel, tid, SnapshotAny, slot)`
    /// (tableam.h): fetch the row version identified by `tid` into `slot`
    /// under SnapshotAny. Returns `false` if no tuple was found.
    pub fn table_tuple_fetch_row_version_any<'mcx>(
        estate: &mut EStateData<'mcx>,
        result_rel_info: RriId,
        tupleid: &ItemPointerData,
        slot: SlotId,
    ) -> PgResult<bool>
);


// `ExecGetChildToRootMap` / `execute_attr_map_slot` / slot-identity copy for the
// cross-partition DELETE save-old path use the already-installed seams from
// `backend-executor-execMain-seams` (`exec_get_child_to_root_map`,
// `exec_get_returning_slot`) and `backend-executor-execTuples-seams`
// (`execute_attr_map_slot`); the tableOid/tid carry is done inline on the EState
// slot. No local seams are needed here.

seam_core::seam!(
    /// `ExecMaterializeSlot(slot)` (execTuples.c): force the slot to hold a
    /// local copy of any pass-by-reference values.
    pub fn exec_materialize_slot<'mcx>(
        estate: &mut EStateData<'mcx>,
        slot: SlotId,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecClearTuple(slot)` (execTuples.c): clear the slot's tuple.
    pub fn exec_clear_tuple<'mcx>(estate: &mut EStateData<'mcx>, slot: SlotId) -> PgResult<()>
);

/// `ExecDelete(context, resultRelInfo, tupleid, oldtuple, processReturning,
/// changingPart, canSetTag, tmresult, tupleDeleted, epqreturnslot)` — delete
/// the tuple identified by `tupleid` (or `oldtuple` for a wholerow/FDW target),
/// running the prologue/act/epilogue and, when `processReturning`, the
/// RETURNING projection. Returns the RETURNING slot or `None`.
pub fn ExecDelete<'mcx>(
    mcx: Mcx<'mcx>,
    context: &mut ModifyTableContext,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    tupleid: Option<&ItemPointerData>,
    oldtuple: Option<FormedTuple<'mcx>>,
    process_returning: bool,
    changing_part: bool,
    can_set_tag: bool,
    mut tmresult: Option<&mut TM_Result>,
    mut tuple_deleted: Option<&mut bool>,
    mut epqreturnslot: Option<&mut Option<SlotId>>,
    // When the EPQ recheck traverses a concurrent-update chain and passes back
    // the re-fetched row via `epqreturnslot`, report the latest (post-traverse)
    // row TID here. This mirrors C's in-place `*tupleid` mutation inside
    // table_tuple_lock(FIND_LAST_VERSION): the cross-partition UPDATE caller
    // must retry against the latest version, not the stale original TID.
    mut advanced_tid: Option<&mut Option<ItemPointerData>>,
) -> PgResult<Option<SlotId>> {
    // TupleTableSlot *slot = NULL;
    let mut slot: Option<SlotId> = None;

    // if (tupleDeleted) *tupleDeleted = false;
    if let Some(td) = tuple_deleted.as_deref_mut().map(|r| &mut *r) {
        *td = false;
    }

    // Prepare for the delete.  This includes BEFORE ROW triggers, so we're done
    // if it says we are. The BEFORE-trigger EPQ recheck (GetTupleForTrigger) may
    // advance the TID to the latest row version; capture it so the cross-partition
    // caller can retry against the current version.
    let mut prologue_advanced_tid: Option<ItemPointerData> = tupleid.copied();
    if !ExecDeletePrologue(
        mcx,
        context,
        mtstate,
        estate,
        result_rel_info,
        tupleid,
        oldtuple.clone(),
        epqreturnslot.as_deref_mut().map(|r| &mut *r),
        tmresult.as_deref_mut().map(|r| &mut *r),
        prologue_advanced_tid.as_mut(),
    )? {
        // Prologue returned false (BEFORE trigger said skip, or its EPQ recheck
        // passed the concurrent tuple back via epqreturnslot). If it ran an EPQ
        // recheck and advanced the TID, report it so the cross-partition caller
        // retries against the latest version (mirrors C's in-place *tupleid).
        if let (Some(adv), Some(eret)) = (
            advanced_tid.as_deref_mut().map(|r| &mut *r),
            epqreturnslot.as_deref_mut().map(|r| &mut *r),
        ) {
            // Only meaningful when an EPQ tuple was actually passed back.
            if eret.is_some() {
                *adv = prologue_advanced_tid;
            }
        }
        return Ok(None);
    }

    // The effective TID being deleted. The plain-table EPQ retry loop advances
    // it to the latest row version (mirroring C's `*tupleid` writeback inside
    // table_tuple_lock); the post-delete Epilogue + RETURNING fetch then use the
    // version that was actually deleted.
    let mut latest_tid: Option<ItemPointerData> = tupleid.copied();

    // INSTEAD OF ROW DELETE Triggers
    if ri_has_instead_delete_row::call(estate, result_rel_info) {
        // Assert(oldtuple != NULL);
        debug_assert!(oldtuple.is_some());
        let dodelete = trigger_seams::exec_ir_delete_triggers::call(
            estate,
            result_rel_info,
            oldtuple.clone(),
        )?;

        if !dodelete {
            // "do nothing"
            return Ok(None);
        }
    } else if ri_has_fdw_routine::call(estate, result_rel_info) {
        // delete from foreign table: let the FDW do it
        //
        // We offer the returning slot as a place to store RETURNING data,
        // although the FDW can return some other slot if it wants.
        let returning = exec_get_returning_slot::call(estate, result_rel_info)?;
        let fdw_slot = fdw_exec_foreign_delete::call(
            estate,
            result_rel_info,
            returning,
            context.planSlot,
        )?;

        let Some(fslot) = fdw_slot else {
            // "do nothing"
            return Ok(None);
        };
        slot = Some(fslot);

        // RETURNING expressions might reference the tableoid column, so
        // (re)initialize tts_tableOid before evaluating them.
        if slot_is_empty::call(estate, fslot) {
            exec_store_all_null_tuple::call(estate, fslot)?;
        }

        let relid = ri_relation_relid::call(estate, result_rel_info);
        slot_set_table_oid::call(estate, fslot, relid);
    } else {
        // delete the tuple
        //
        // Note: if context->estate->es_crosscheck_snapshot isn't
        // InvalidSnapshot, we check that the row to be deleted is visible to
        // that snapshot, and throw a can't-serialize error if not. This is a
        // special-case behavior needed for referential integrity updates in
        // transaction-snapshot mode transactions.
        //
        // C label `ldelete:` is reached again only on the TM_Updated→TM_Ok
        // retry, so this is a loop.
        // `tupleid` is updated to the latest row version on each EPQ retry (C
        // updates `*tupleid` inside `table_tuple_lock` with FIND_LAST_VERSION).
        let mut cur_tid = *tupleid.expect("ExecDelete: heap delete requires a TID");
        loop {
            // result = ExecDeleteAct(context, resultRelInfo, tupleid, changingPart);
            let result =
                ExecDeleteAct(context, estate, result_rel_info, &cur_tid, changing_part)?;

            // if (tmresult) *tmresult = result;
            if let Some(tr) = tmresult.as_deref_mut().map(|r| &mut *r) {
                *tr = result;
            }

            match result {
                TM_Result::TM_SelfModified => {
                    // The target tuple was already updated or deleted by the
                    // current command, or by a later command in the current
                    // transaction.  See the C comment block for the rationale.
                    if context.tmfd.cmax != estate.es_output_cid {
                        return Err(PgError::error(
                            "tuple to be deleted was already modified by an operation triggered by the current command",
                        )
                        .with_sqlstate(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION)
                        .with_hint(
                            "Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.",
                        ));
                    }

                    // Else, already deleted by self; nothing to do
                    return Ok(None);
                }

                TM_Result::TM_Ok => break,

                TM_Result::TM_Updated => {
                    if isolation_uses_xact_snapshot::call() {
                        return Err(PgError::error(
                            "could not serialize access due to concurrent update",
                        )
                        .with_sqlstate(ERRCODE_T_R_SERIALIZATION_FAILURE));
                    }

                    // Already know that we're going to need to do EPQ, so fetch
                    // tuple directly into the right slot.
                    eval_plan_qual_begin::call(mtstate)?;
                    let rti = estate.result_rel(result_rel_info).ri_RangeTableIndex;
                    let inputslot =
                        eval_plan_qual_slot::call(estate, mtstate, result_rel_info, rti)?;

                    let snapshot = es_snapshot::call(estate);
                    let lock_result = table_tuple_lock::call(
                        estate,
                        result_rel_info,
                        &cur_tid,
                        snapshot,
                        inputslot,
                        estate.es_output_cid,
                        LockTupleExclusive,
                        LockWaitPolicy::LockWaitBlock,
                        // TUPLE_LOCK_FLAG_FIND_LAST_VERSION
                        true,
                        &mut context.tmfd,
                    )?;

                    match lock_result {
                        TM_Result::TM_Ok => {
                            debug_assert!(context.tmfd.traversed);
                            // C's table_tuple_lock updates *tupleid to the latest
                            // locked version; the owned lock leaves it in the
                            // slot's tts_tid. Copy it back so the `ldelete` retry
                            // deletes the new version (else it re-locks the old
                            // TID → TM_Updated forever).
                            cur_tid = estate.slot(inputslot).tts_tid;
                            latest_tid = Some(cur_tid);
                            let epqslot = eval_plan_qual::call(
                                estate,
                                mtstate,
                                result_rel_info,
                                inputslot,
                            )?;
                            match epqslot {
                                None => {
                                    // Tuple not passing quals anymore, exiting...
                                    return Ok(None);
                                }
                                Some(epqslot) => {
                                    // If requested, skip delete and pass back the
                                    // updated row.
                                    if let Some(epqret) =
                                        epqreturnslot.as_deref_mut().map(|r| &mut *r)
                                    {
                                        *epqret = Some(epqslot);
                                        // Report the latest (traversed) TID so the
                                        // cross-partition caller retries against the
                                        // current row version (C: in-place *tupleid).
                                        if let Some(adv) =
                                            advanced_tid.as_deref_mut().map(|r| &mut *r)
                                        {
                                            *adv = Some(cur_tid);
                                        }
                                        return Ok(None);
                                    } else {
                                        // goto ldelete;
                                        continue;
                                    }
                                }
                            }
                        }

                        TM_Result::TM_SelfModified => {
                            // Reached when following an update chain from a
                            // tuple updated by another session, reaching a
                            // tuple already updated in this transaction. If
                            // previously updated by this command, ignore the
                            // delete, otherwise error out.
                            if context.tmfd.cmax != estate.es_output_cid {
                                return Err(PgError::error(
                                    "tuple to be deleted was already modified by an operation triggered by the current command",
                                )
                                .with_sqlstate(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION)
                                .with_hint(
                                    "Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.",
                                ));
                            }
                            return Ok(None);
                        }

                        TM_Result::TM_Deleted => {
                            // tuple already deleted; nothing to do
                            return Ok(None);
                        }

                        other => {
                            // TM_Invisible / TM_Updated are impossible here
                            // (waiting for the latest version via
                            // TUPLE_LOCK_FLAG_FIND_LAST_VERSION).
                            return Err(PgError::error(format!(
                                "unexpected table_tuple_lock status: {:?}",
                                other
                            )));
                        }
                    }
                }

                TM_Result::TM_Deleted => {
                    if isolation_uses_xact_snapshot::call() {
                        return Err(PgError::error(
                            "could not serialize access due to concurrent delete",
                        )
                        .with_sqlstate(ERRCODE_T_R_SERIALIZATION_FAILURE));
                    }
                    // tuple already deleted; nothing to do
                    return Ok(None);
                }

                other => {
                    return Err(PgError::error(format!(
                        "unrecognized table_tuple_delete status: {:?}",
                        other
                    )));
                }
            }
        }

        // Note: Normally one would think that we have to delete index tuples
        // associated with the heap tuple now... but VACUUM takes care of it.
    }

    // if (canSetTag) (estate->es_processed)++;
    if can_set_tag {
        estate.es_processed += 1;
    }

    // Tell caller that the delete actually happened.
    if let Some(td) = tuple_deleted.as_deref_mut().map(|r| &mut *r) {
        *td = true;
    }

    ExecDeleteEpilogue(
        mcx,
        context,
        mtstate,
        estate,
        result_rel_info,
        latest_tid.as_ref().or(tupleid),
        oldtuple.clone(),
        changing_part,
    )?;

    // Process RETURNING if present and if requested.
    //
    // If this is part of a cross-partition UPDATE, and the RETURNING list
    // refers to any OLD column values, save the old tuple here for later
    // processing of the RETURNING list by ExecInsert().
    let save_old = changing_part
        && ri_has_project_returning::call(estate, result_rel_info)
        && ri_returning_has_old::call(estate, result_rel_info);

    if ri_has_project_returning::call(estate, result_rel_info) && (process_returning || save_old) {
        // We have to put the target tuple into a slot, which means first we
        // gotta fetch it.  We can use the trigger tuple slot.
        if ri_has_fdw_routine::call(estate, result_rel_info) {
            // FDW must have provided a slot containing the deleted row
            debug_assert!(slot.is_some_and(|s| !slot_is_null::call(estate, s)));
        } else {
            let rslot = exec_get_returning_slot::call(estate, result_rel_info)?;
            slot = Some(rslot);
            if let Some(ot) = oldtuple.as_ref() {
                let mcx = estate.es_query_cxt;
                let formed = ot.clone_in(mcx)?;
                execTuples_seams::exec_force_store_formed_heap_tuple::call(
                    estate, rslot, formed, false,
                )?;
            } else {
                let tupleid = latest_tid
                    .as_ref()
                    .or(tupleid)
                    .expect("DELETE RETURNING fetch requires a TID");
                if !table_tuple_fetch_row_version_any::call(
                    estate,
                    result_rel_info,
                    tupleid,
                    rslot,
                )? {
                    return Err(PgError::error(
                        "failed to fetch deleted tuple for DELETE RETURNING",
                    ));
                }
            }
        }

        // If required, save the old tuple for later processing of the RETURNING
        // list by ExecInsert().
        if save_old {
            // Convert the tuple into the root partition's format/slot, if
            // needed.  ExecInsert() will then convert it to the new partition's
            // format/slot, if necessary.
            // tupconv_map = ExecGetChildToRootMap(resultRelInfo);
            // if (tupconv_map != NULL)
            if execMain_seams::exec_get_child_to_root_map::call(
                estate,
                result_rel_info,
            )? {
                let root_rel_info = mtstate
                    .rootResultRelInfo
                    .expect("cross-partition DELETE save-old requires a root ResultRelInfo");
                let old_slot = slot.expect("save-old slot must be set");

                // slot = execute_attr_map_slot(tupconv_map->attrMap, slot,
                //          ExecGetReturningSlot(estate, rootRelInfo));
                let out_slot = execMain_seams::exec_get_returning_slot::call(
                    estate,
                    root_rel_info,
                )?;
                let converted = execTuples_seams::execute_attr_map_slot::call(
                    estate,
                    result_rel_info,
                    old_slot,
                    out_slot,
                )?;

                // slot->tts_tableOid = oldSlot->tts_tableOid;
                // ItemPointerCopy(&oldSlot->tts_tid, &slot->tts_tid);
                let (src_oid, src_tid) = {
                    let s = estate.slot(old_slot);
                    (s.tts_tableOid, s.tts_tid)
                };
                let dst = estate.slot_mut(converted);
                dst.tts_tableOid = src_oid;
                dst.tts_tid = src_tid;

                slot = Some(converted);
            }

            context.cpDeletedSlot = slot;

            return Ok(None);
        }

        let plan_slot = context.planSlot;
        let rslot = ExecProcessReturning(
            estate,
            result_rel_info,
            CmdType::CMD_DELETE,
            slot,
            None,
            plan_slot,
        )?;

        // Before releasing the target tuple again, make sure rslot has a local
        // copy of any pass-by-reference values.
        exec_materialize_slot::call(estate, rslot)?;

        if let Some(s) = slot {
            exec_clear_tuple::call(estate, s)?;
        }

        return Ok(Some(rslot));
    }

    Ok(None)
}
