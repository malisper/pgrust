//! UPDATE family of `executor/nodeModifyTable.c`: the prologue/act/epilogue
//! sequence, the cross-partition UPDATE (delete-then-insert + foreign-key
//! bookkeeping), the new-tuple projection setup, and the `ExecUpdate` driver.

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::nodes::CmdType;
use types_nodes::{EStateData, ModifyTableState, RriId, SlotId};
use types_snapshot::{SnapshotData, SnapshotType};
use types_tableam::tableam::{
    LockWaitPolicy, TM_Result, TU_UpdateIndexes, TUPLE_LOCK_FLAG_FIND_LAST_VERSION,
};
use types_tuple::heaptuple::{HeapTuple, ItemPointerData};

use crate::lifecycle::ExecProcessReturning;
use crate::{ModifyTableContext, UpdateContext};

/// `WCO_RLS_UPDATE_CHECK` (parsenodes.h `WCOKind`).
const WCO_RLS_UPDATE_CHECK: i32 = 2;
/// `WCO_VIEW_CHECK` (parsenodes.h `WCOKind`).
const WCO_VIEW_CHECK: i32 = 0;
/// `InplaceUpdateTupleLock` == `ExclusiveLock` (lockdefs.h). `LOCKMODE` is
/// `i32`.
const INPLACE_UPDATE_TUPLE_LOCK: i32 = 7;

/// `SnapshotAny` (snapmgr) — the static "any tuple visible" snapshot.
fn snapshot_any() -> Option<SnapshotData> {
    Some(SnapshotData::sentinel(SnapshotType::SNAPSHOT_ANY))
}

/// `ExecUpdate(context, resultRelInfo, tupleid, oldtuple, oldSlot, slot,
/// canSetTag)` — update the tuple identified by `tupleid`/`oldtuple` to the
/// contents of `slot`, running the prologue/act/epilogue (with a possible
/// cross-partition route) and the RETURNING projection. Returns the RETURNING
/// slot or `None`.
pub fn ExecUpdate<'mcx>(
    mcx: Mcx<'mcx>,
    context: &mut ModifyTableContext,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    tupleid: Option<&ItemPointerData>,
    oldtuple: HeapTuple<'mcx>,
    mut old_slot: Option<SlotId>,
    mut slot: SlotId,
    can_set_tag: bool,
) -> PgResult<Option<SlotId>> {
    let mut update_cxt = UpdateContext {
        crossPartUpdate: false,
        updateIndexes: TU_UpdateIndexes::TU_None,
        // C: zero-initialized stack field, overwritten by ExecUpdateAct.
        lockmode: types_tableam::tableam::LockTupleMode::LockTupleKeyShare,
    };

    let result_relation_oid = relation_oid(estate, result_rel_info);

    // abort the operation if not running transactions
    if backend_utils_init_miscinit_seams::is_bootstrap_processing_mode::call() {
        return Err(types_error::PgError::error(
            "cannot UPDATE during bootstrap",
        ));
    }

    // Prepare for the update.  This includes BEFORE ROW triggers, so we're done
    // if it says we are.
    if !ExecUpdatePrologue(
        mcx,
        context,
        mtstate,
        estate,
        result_rel_info,
        tupleid,
        oldtuple.clone(),
        slot,
        None,
    )? {
        return Ok(None);
    }

    let trig_instead = estate
        .result_rel(result_rel_info)
        .ri_trig_update_instead_row;
    let has_fdw = estate.result_rel(result_rel_info).ri_has_fdw_routine;

    if trig_instead {
        // INSTEAD OF ROW UPDATE Triggers
        if !backend_commands_trigger_seams::exec_ir_update_triggers::call(
            estate,
            result_rel_info,
            oldtuple.clone(),
            slot,
        )? {
            return Ok(None); // "do nothing"
        }
    } else if has_fdw {
        // Fill in GENERATEd columns
        ExecUpdatePrepareSlot(mcx, estate, result_rel_info, slot)?;

        // update in foreign table: let the FDW do it
        let plan_slot = context.planSlot;
        match backend_executor_execMain_seams::exec_foreign_update::call(
            estate,
            result_rel_info,
            slot,
            plan_slot,
        )? {
            None => return Ok(None), // "do nothing"
            Some(s) => {
                slot = s;
                // AFTER ROW Triggers or RETURNING expressions might reference
                // the tableoid column, so (re-)initialize tts_tableOid before
                // evaluating them.
                estate.slot_mut(slot).tts_tableOid = result_relation_oid;
            }
        }
    } else {
        // If we generate a new candidate tuple after EvalPlanQual testing, we
        // must loop back here to try again.
        let mut lockedtid: ItemPointerData;
        // tupleid must be valid in this branch (plain table update).
        let mut cur_tid = *tupleid.expect("ExecUpdate: plain table update needs tupleid");

        let result: TM_Result = loop {
            // redo_act:
            lockedtid = cur_tid;
            let r = ExecUpdateAct(
                mcx,
                context,
                mtstate,
                estate,
                result_rel_info,
                Some(&cur_tid),
                oldtuple.clone(),
                slot,
                can_set_tag,
                &mut update_cxt,
            )?;

            // If ExecUpdateAct reports that a cross-partition update was done,
            // then the RETURNING tuple (if any) has been projected and there's
            // nothing else for us to do.
            if update_cxt.crossPartUpdate {
                return Ok(context.cpUpdateReturningSlot);
            }

            match r {
                TM_Result::TM_SelfModified => {
                    if context.tmfd.cmax != estate.es_output_cid {
                        return Err(types_error::PgError::error(
                            "tuple to be updated was already modified by an operation triggered by the current command",
                        )
                        .with_sqlstate(types_error::ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION)
                        .with_hint(
                            "Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.",
                        ));
                    }
                    // Else, already updated by self; nothing to do
                    return Ok(None);
                }
                TM_Result::TM_Ok => break TM_Result::TM_Ok,
                TM_Result::TM_Updated => {
                    if backend_access_transam_xact_seams::isolation_uses_xact_snapshot::call() {
                        return Err(types_error::PgError::error(
                            "could not serialize access due to concurrent update",
                        )
                        .with_sqlstate(types_error::ERRCODE_T_R_SERIALIZATION_FAILURE));
                    }

                    let rti = estate.result_rel(result_rel_info).ri_RangeTableIndex;

                    // Already know that we're going to need to do EPQ, so fetch
                    // tuple directly into the right slot.
                    let inputslot = backend_executor_execMain_seams::eval_plan_qual_slot::call(
                        estate,
                        &mut mtstate.mt_epqstate,
                        result_rel_info,
                        rti,
                    )?;

                    let rel = relation_alias(estate, result_rel_info);
                    let snapshot = estate.es_snapshot.as_deref().cloned();
                    let cid = estate.es_output_cid;
                    let lockmode = update_cxt.lockmode;
                    let inslot = estate.slot_mut(inputslot);
                    let r2 = backend_access_table_tableam::table_tuple_lock(
                        &rel,
                        &cur_tid,
                        &snapshot,
                        inslot,
                        cid,
                        lockmode,
                        LockWaitPolicy::LockWaitBlock,
                        TUPLE_LOCK_FLAG_FIND_LAST_VERSION,
                        &mut context.tmfd,
                    )?;

                    match r2 {
                        TM_Result::TM_Ok => {
                            // Assert(context->tmfd.traversed);
                            let epqslot = backend_executor_execMain_seams::eval_plan_qual::call(
                                estate,
                                &mut mtstate.mt_epqstate,
                                result_rel_info,
                                rti,
                                inputslot,
                            )?;
                            let epqslot = match epqslot {
                                None => return Ok(None), // not passing quals anymore
                                Some(s) => s,
                            };

                            // Make sure ri_oldTupleSlot is initialized.
                            if !estate.result_rel(result_rel_info).ri_projectNewInfoValid {
                                ExecInitUpdateProjection(mcx, mtstate, estate, result_rel_info)?;
                            }

                            if estate.result_rel(result_rel_info).ri_needLockTagTuple {
                                backend_storage_lmgr_lmgr_seams::unlock_tuple::call(
                                    result_relation_oid,
                                    lockedtid,
                                    INPLACE_UPDATE_TUPLE_LOCK,
                                )?;
                                backend_storage_lmgr_lmgr_seams::lock_tuple::call(
                                    result_relation_oid,
                                    cur_tid,
                                    INPLACE_UPDATE_TUPLE_LOCK,
                                )?;
                            }

                            // Fetch the most recent version of old tuple.
                            let old_tuple_slot =
                                estate.result_rel(result_rel_info).ri_oldTupleSlot.expect(
                                    "ExecUpdate: ri_oldTupleSlot not initialized after ExecInitUpdateProjection",
                                );
                            let rel2 = relation_alias(estate, result_rel_info);
                            let any = snapshot_any();
                            let oldslot_ref = estate.slot_mut(old_tuple_slot);
                            if !backend_access_table_tableam::table_tuple_fetch_row_version(
                                &rel2, &cur_tid, &any, oldslot_ref,
                            )? {
                                return Err(types_error::PgError::error(
                                    "failed to fetch tuple being updated",
                                ));
                            }
                            old_slot = Some(old_tuple_slot);
                            slot = ExecGetUpdateNewTuple(
                                estate,
                                result_rel_info,
                                epqslot,
                                Some(old_tuple_slot),
                            )?;
                            // goto redo_act
                            continue;
                        }
                        TM_Result::TM_Deleted => return Ok(None),
                        TM_Result::TM_SelfModified => {
                            if context.tmfd.cmax != estate.es_output_cid {
                                return Err(types_error::PgError::error(
                                    "tuple to be updated was already modified by an operation triggered by the current command",
                                )
                                .with_sqlstate(types_error::ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION)
                                .with_hint(
                                    "Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.",
                                ));
                            }
                            return Ok(None);
                        }
                        other => {
                            return Err(types_error::PgError::error(format!(
                                "unexpected table_tuple_lock status: {}",
                                other as u32
                            )));
                        }
                    }
                }
                TM_Result::TM_Deleted => {
                    if backend_access_transam_xact_seams::isolation_uses_xact_snapshot::call() {
                        return Err(types_error::PgError::error(
                            "could not serialize access due to concurrent delete",
                        )
                        .with_sqlstate(types_error::ERRCODE_T_R_SERIALIZATION_FAILURE));
                    }
                    // tuple already deleted; nothing to do
                    return Ok(None);
                }
                other => {
                    return Err(types_error::PgError::error(format!(
                        "unrecognized table_tuple_update status: {}",
                        other as u32
                    )));
                }
            }
        };
        let _ = result;
        let _ = &mut cur_tid;
    }

    if can_set_tag {
        estate.es_processed += 1;
    }

    ExecUpdateEpilogue(
        mcx,
        context,
        mtstate,
        estate,
        &update_cxt,
        result_rel_info,
        tupleid,
        oldtuple,
        slot,
    )?;

    // Process RETURNING if present
    if estate.result_rel(result_rel_info).ri_has_project_returning {
        return Ok(Some(ExecProcessReturning(
            estate,
            result_rel_info,
            CmdType::CMD_UPDATE,
            old_slot,
            Some(slot),
            context.planSlot,
        )?));
    }

    Ok(None)
}

/// `ExecUpdatePrologue(context, resultRelInfo, tupleid, oldtuple, slot,
/// result)` — recheck the partition constraint, fire BEFORE ROW UPDATE
/// triggers (or dispatch the FDW), returning `false` to skip the update.
pub fn ExecUpdatePrologue<'mcx>(
    mcx: Mcx<'mcx>,
    context: &mut ModifyTableContext,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    tupleid: Option<&ItemPointerData>,
    oldtuple: HeapTuple<'mcx>,
    slot: SlotId,
    result: Option<&mut TM_Result>,
) -> PgResult<bool> {
    if let Some(r) = result {
        *r = TM_Result::TM_Ok;
    }

    backend_executor_execTuples_seams::exec_materialize_slot::call(estate, slot)?;

    // Open the table's indexes, if we have not done so already, so that we can
    // add new index entries for the updated tuple.
    let relhasindex = relation_relhasindex(estate, result_rel_info);
    let indexes_open = estate
        .result_rel(result_rel_info)
        .ri_IndexRelationDescs
        .is_some();
    if relhasindex && !indexes_open {
        backend_executor_execIndexing_seams::exec_open_indices::call(
            estate,
            result_rel_info,
            false,
        )?;
    }

    // BEFORE ROW UPDATE triggers
    let trig_before = estate.result_rel(result_rel_info).ri_trig_update_before_row;
    if trig_before {
        // Flush any pending inserts, so rows are visible to the triggers
        if !estate.es_insert_pending_result_relations.is_empty() {
            crate::insert::ExecPendingInserts(mcx, estate)?;
        }

        let is_merge = mtstate.operation == CmdType::CMD_MERGE;
        // The seam moves the BEFORE-trigger TM_Result back through `tmfd`'s
        // sibling output; here we pass `None` (ExecUpdate's caller passed NULL).
        return backend_commands_trigger_seams::exec_br_update_triggers::call(
            estate,
            &mut mtstate.mt_epqstate,
            result_rel_info,
            tupleid,
            oldtuple,
            slot,
            None,
            &mut context.tmfd,
            is_merge,
        );
    }

    Ok(true)
}

/// `ExecUpdatePrepareSlot(resultRelInfo, slot, estate)` — compute stored
/// generated columns into `slot` before the update is performed.
pub fn ExecUpdatePrepareSlot<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    slot: SlotId,
) -> PgResult<()> {
    // Constraints and GENERATED expressions might reference the tableoid
    // column, so (re-)initialize tts_tableOid before evaluating them.
    let reloid = relation_oid(estate, result_rel_info);
    estate.slot_mut(slot).tts_tableOid = reloid;

    // Compute stored generated columns
    if relation_has_generated_stored(estate, result_rel_info) {
        crate::lifecycle::ExecComputeStoredGenerated(
            mcx,
            estate,
            result_rel_info,
            slot,
            CmdType::CMD_UPDATE,
        )?;
    }

    Ok(())
}

/// `ExecUpdateAct(context, resultRelInfo, tupleid, oldtuple, slot, canSetTag,
/// updateCxt)` — perform the actual `table_tuple_update` (or route to a
/// cross-partition update / FDW), filling `updateCxt` with the outcome and
/// returning its `TM_Result`.
pub fn ExecUpdateAct<'mcx>(
    mcx: Mcx<'mcx>,
    context: &mut ModifyTableContext,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    tupleid: Option<&ItemPointerData>,
    oldtuple: HeapTuple<'mcx>,
    mut slot: SlotId,
    can_set_tag: bool,
    update_cxt: &mut UpdateContext,
) -> PgResult<TM_Result> {
    update_cxt.crossPartUpdate = false;

    let result: TM_Result = loop {
        // lreplace:
        // Fill in GENERATEd columns
        ExecUpdatePrepareSlot(mcx, estate, result_rel_info, slot)?;

        // ensure slot is independent, consider e.g. EPQ
        backend_executor_execTuples_seams::exec_materialize_slot::call(estate, slot)?;

        // If partition constraint fails, this row might get moved to another
        // partition, in which case we should check the RLS CHECK policy just
        // before inserting into the new partition.  So skip the WCO checks if
        // the partition constraint fails.
        let relispartition = relation_relispartition(estate, result_rel_info);
        let partition_constraint_failed = relispartition
            && !backend_executor_execMain_seams::exec_partition_check::call(
                estate,
                result_rel_info,
                slot,
                false,
            )?;

        // Check any RLS UPDATE WITH CHECK policies
        if !partition_constraint_failed
            && estate.result_rel(result_rel_info).ri_has_with_check_options
        {
            backend_executor_execMain_seams::exec_with_check_options::call(
                estate,
                WCO_RLS_UPDATE_CHECK,
                result_rel_info,
                slot,
            )?;
        }

        // If a partition check failed, try to move the row into the right
        // partition.
        if partition_constraint_failed {
            let mut inserted_tuple: Option<SlotId> = None;
            let mut retry_slot: Option<SlotId> = None;
            let mut insert_destrel: Option<RriId> = None;
            let mut result = TM_Result::TM_Ok;

            if ExecCrossPartitionUpdate(
                mcx,
                context,
                mtstate,
                estate,
                result_rel_info,
                tupleid,
                oldtuple.clone(),
                slot,
                can_set_tag,
                update_cxt,
                &mut result,
                &mut retry_slot,
                &mut inserted_tuple,
                &mut insert_destrel,
            )? {
                // success!
                update_cxt.crossPartUpdate = true;

                // If the partitioned table being updated is referenced in
                // foreign keys, queue up trigger events to check that none of
                // them were violated.
                let trig_after = estate.result_rel(result_rel_info).ri_trig_update_after_row;
                if let Some(dest) = insert_destrel {
                    if trig_after {
                        if let (Some(tid), Some(ins)) = (tupleid, inserted_tuple) {
                            ExecCrossPartitionUpdateForeignKey(
                                mcx,
                                context,
                                mtstate,
                                estate,
                                result_rel_info,
                                dest,
                                tid,
                                slot,
                                ins,
                            )?;
                        }
                    }
                }

                return Ok(TM_Result::TM_Ok);
            }

            // No luck, a retry is needed.  If running MERGE, we do not do so
            // here; instead let it handle that on its own rules.
            if mtstate.operation == CmdType::CMD_MERGE {
                return Ok(result);
            }

            // ExecCrossPartitionUpdate installed an updated version of the new
            // tuple in the retry slot; start over.
            slot = retry_slot.expect("ExecUpdateAct: cross-partition retry without retry_slot");
            continue; // goto lreplace
        }

        // Check the constraints of the tuple.
        if relation_has_constr(estate, result_rel_info) {
            backend_executor_execMain_seams::exec_constraints::call(
                estate,
                result_rel_info,
                slot,
            )?;
        }

        // replace the heap tuple
        let rel = relation_alias(estate, result_rel_info);
        let cid = estate.es_output_cid;
        let snapshot = estate.es_snapshot.as_deref().cloned();
        let crosscheck = estate.es_crosscheck_snapshot.as_deref().cloned();
        let otid = *tupleid.expect("ExecUpdateAct: plain table update needs tupleid");
        let slot_ref = estate.slot_mut(slot);
        break backend_access_table_tableam::table_tuple_update(
            &rel,
            &otid,
            slot_ref,
            cid,
            &snapshot,
            &crosscheck,
            true, // wait for commit
            &mut context.tmfd,
            &mut update_cxt.lockmode,
            &mut update_cxt.updateIndexes,
        )?;
    };

    Ok(result)
}

/// `ExecUpdateEpilogue(context, updateCxt, resultRelInfo, tupleid, oldtuple,
/// slot)` — maintain indexes, fire AFTER ROW UPDATE triggers, and capture the
/// OLD/NEW tuples for transition tables.
pub fn ExecUpdateEpilogue<'mcx>(
    mcx: Mcx<'mcx>,
    _context: &mut ModifyTableContext,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    update_cxt: &UpdateContext,
    result_rel_info: RriId,
    tupleid: Option<&ItemPointerData>,
    oldtuple: HeapTuple<'mcx>,
    slot: SlotId,
) -> PgResult<()> {
    let _ = &oldtuple;
    let mut recheck_indexes: mcx::PgVec<'mcx, types_core::Oid> = mcx::vec_with_capacity_in(mcx, 0)?;

    // insert index entries for tuple if necessary
    let num_indices = estate.result_rel(result_rel_info).ri_NumIndices;
    if num_indices > 0 && update_cxt.updateIndexes != TU_UpdateIndexes::TU_None {
        let only_summarizing = update_cxt.updateIndexes == TU_UpdateIndexes::TU_Summarizing;
        recheck_indexes = backend_executor_execIndexing_seams::exec_insert_index_tuples::call(
            mcx,
            estate,
            result_rel_info,
            slot,
            true,
            false,
            None,
            &[],
            only_summarizing,
        )?;
    }

    // AFTER ROW UPDATE Triggers
    let use_oc_capture = mtstate.operation == CmdType::CMD_INSERT;
    let capture = if use_oc_capture {
        mtstate.mt_oc_transition_capture.as_deref_mut()
    } else {
        mtstate.mt_transition_capture.as_deref_mut()
    };
    backend_commands_trigger_seams::exec_ar_update_triggers::call(
        estate,
        result_rel_info,
        None, // src_partinfo == NULL
        None, // dst_partinfo == NULL
        tupleid,
        None,
        Some(slot),
        &recheck_indexes,
        capture,
        false,
    )?;

    // Check any WITH CHECK OPTION constraints from parent views.
    if estate.result_rel(result_rel_info).ri_has_with_check_options {
        backend_executor_execMain_seams::exec_with_check_options::call(
            estate,
            WCO_VIEW_CHECK,
            result_rel_info,
            slot,
        )?;
    }

    Ok(())
}

/// `ExecCrossPartitionUpdate(...)` — implement an UPDATE that moves a row to a
/// different partition by deleting from the source partition and inserting into
/// the target. Returns `false` (with `retry_slot` set) when the delete saw a
/// concurrent modification and the operation must be retried.
pub fn ExecCrossPartitionUpdate<'mcx>(
    mcx: Mcx<'mcx>,
    context: &mut ModifyTableContext,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    tupleid: Option<&ItemPointerData>,
    oldtuple: HeapTuple<'mcx>,
    mut slot: SlotId,
    can_set_tag: bool,
    _update_cxt: &mut UpdateContext,
    tmresult: &mut TM_Result,
    retry_slot: &mut Option<SlotId>,
    inserted_tuple: &mut Option<SlotId>,
    insert_destrel: &mut Option<RriId>,
) -> PgResult<bool> {
    context.cpDeletedSlot = None;
    context.cpUpdateReturningSlot = None;
    *retry_slot = None;

    // Disallow an INSERT ON CONFLICT DO UPDATE that causes the original row to
    // migrate to a different partition.
    if mtstate_on_conflict_is_update(mtstate) {
        return Err(types_error::PgError::error("invalid ON UPDATE specification")
            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .with_detail(
                "The result tuple would appear in a different partition than the original tuple.",
            ));
    }

    // When an UPDATE is run directly on a leaf partition, simply fail with a
    // partition constraint violation error.
    if Some(result_rel_info) == mtstate.rootResultRelInfo {
        backend_executor_execMain_seams::exec_partition_check_emit_error::call(
            estate,
            result_rel_info,
            slot,
        )?;
    }

    // Initialize tuple routing info if not already done.
    if mtstate.mt_partition_tuple_routing.is_none() {
        let root_rri = mtstate
            .rootResultRelInfo
            .expect("ExecCrossPartitionUpdate: no rootResultRelInfo");
        // Things built here have to last for the query duration (es_query_cxt).
        let qcxt = estate.es_query_cxt;
        let root_rel = relation_alias(estate, root_rri);
        let proute = backend_executor_execPartition_seams::exec_setup_partition_tuple_routing::call(
            qcxt,
            estate,
            root_rel.alias(),
        )?;
        mtstate.mt_partition_tuple_routing = Some(proute);

        // Before a partition's tuple can be re-routed, it must first be
        // converted to the root's format, so we'll need a slot for storing such
        // tuples.
        debug_assert!(mtstate.mt_root_tuple_slot.is_none());
        let root_slot = backend_access_table_tableam::table_slot_create(qcxt, &root_rel)?;
        let root_slot_id = estate.make_slot(root_slot)?;
        mtstate.mt_root_tuple_slot = Some(root_slot_id);
    }

    // Row movement, part 1.  Delete the tuple, but skip RETURNING processing.
    let mut tuple_deleted = false;
    let mut epqslot: Option<SlotId> = None;
    crate::delete_exec::ExecDelete(
        mcx,
        context,
        mtstate,
        estate,
        result_rel_info,
        tupleid,
        oldtuple,
        false, // processReturning
        true,  // changingPart
        false, // canSetTag
        Some(tmresult),
        Some(&mut tuple_deleted),
        Some(&mut epqslot),
    )?;

    // For some reason if DELETE didn't happen then we should skip the insert as
    // well; otherwise an UPDATE could increase the total number of rows.
    if !tuple_deleted {
        if mtstate.operation == CmdType::CMD_MERGE {
            return Ok(*tmresult == TM_Result::TM_Ok);
        } else if epqslot.is_none() || estate.slot(epqslot.unwrap()).is_empty() {
            return Ok(true);
        } else {
            let epqslot = epqslot.unwrap();
            // Fetch the most recent version of old tuple.
            // ... but first, make sure ri_oldTupleSlot is initialized.
            if !estate.result_rel(result_rel_info).ri_projectNewInfoValid {
                ExecInitUpdateProjection(mcx, mtstate, estate, result_rel_info)?;
            }
            let old_slot = estate.result_rel(result_rel_info).ri_oldTupleSlot.expect(
                "ExecCrossPartitionUpdate: ri_oldTupleSlot not initialized",
            );
            let rel = relation_alias(estate, result_rel_info);
            let any = snapshot_any();
            let tid = *tupleid.expect("ExecCrossPartitionUpdate: needs tupleid");
            let oldslot_ref = estate.slot_mut(old_slot);
            if !backend_access_table_tableam::table_tuple_fetch_row_version(
                &rel, &tid, &any, oldslot_ref,
            )? {
                return Err(types_error::PgError::error(
                    "failed to fetch tuple being updated",
                ));
            }
            // and project the new tuple to retry the UPDATE with
            *retry_slot = Some(ExecGetUpdateNewTuple(
                estate,
                result_rel_info,
                epqslot,
                Some(old_slot),
            )?);
            return Ok(false);
        }
    }

    // resultRelInfo is one of the per-relation resultRelInfos.  So we should
    // convert the tuple into root's tuple descriptor if needed.
    if backend_executor_execMain_seams::exec_get_child_to_root_map::call(estate, result_rel_info)? {
        let root_slot = mtstate
            .mt_root_tuple_slot
            .expect("ExecCrossPartitionUpdate: mt_root_tuple_slot not set");
        slot = backend_executor_execTuples_seams::execute_attr_map_slot::call(
            estate,
            result_rel_info,
            slot,
            root_slot,
        )?;
    }

    // Tuple routing starts from the root table.
    let root_rri = mtstate
        .rootResultRelInfo
        .expect("ExecCrossPartitionUpdate: no rootResultRelInfo");
    context.cpUpdateReturningSlot = crate::insert_exec::ExecInsert(
        mcx,
        context,
        mtstate,
        estate,
        root_rri,
        slot,
        can_set_tag,
        Some(inserted_tuple),
        Some(insert_destrel),
    )?;

    // Reset the transition state that may possibly have been written by INSERT.
    if let Some(tc) = mtstate.mt_transition_capture.as_deref_mut() {
        tc.tcs_original_insert_tuple = None;
    }

    // We're done moving.
    Ok(true)
}

/// `ExecCrossPartitionUpdateForeignKey(...)` — fire the AFTER ROW UPDATE
/// foreign-key triggers for a row moved across partitions.
pub fn ExecCrossPartitionUpdateForeignKey<'mcx>(
    mcx: Mcx<'mcx>,
    _context: &mut ModifyTableContext,
    _mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    source_part_info: RriId,
    dest_part_info: RriId,
    tupleid: &ItemPointerData,
    _oldslot: SlotId,
    newslot: SlotId,
) -> PgResult<()> {
    let root_rel_info = estate
        .result_rel(source_part_info)
        .ri_RootResultRelInfo
        .expect("ExecCrossPartitionUpdateForeignKey: source has no root");
    let ancestor_rels = backend_executor_execMain_seams::exec_get_ancestor_result_rels::call(
        mcx,
        estate,
        source_part_info,
    )?;

    // For any foreign keys that point directly into a non-root ancestor of the
    // source partition, report an error that those cannot be enforced.
    for r_info in ancestor_rels.iter().copied() {
        // Root ancestor's triggers will be processed.
        if r_info == root_rel_info {
            continue;
        }

        let mut has_noncloned_fkey = false;
        if estate.result_rel(r_info).ri_trig_update_after_row {
            has_noncloned_fkey =
                backend_commands_trigger_seams::has_noncloned_pk_fkey_trigger::call(
                    estate, r_info,
                )?;
        }

        if has_noncloned_fkey {
            let rinfo_name = relation_name(estate, r_info);
            let root_name = relation_name(estate, root_rel_info);
            return Err(types_error::PgError::error(
                "cannot move tuple across partitions when a non-root ancestor of the source partition is directly referenced in a foreign key",
            )
            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .with_detail(format!(
                "A foreign key points to ancestor \"{}\" but not the root ancestor \"{}\".",
                rinfo_name, root_name
            ))
            .with_hint(format!(
                "Consider defining the foreign key on table \"{}\".",
                root_name
            )));
        }
    }

    // Perform the root table's triggers.
    backend_commands_trigger_seams::exec_ar_update_triggers::call(
        estate,
        root_rel_info,
        Some(source_part_info),
        Some(dest_part_info),
        Some(tupleid),
        None,
        Some(newslot),
        &[],
        None,
        true,
    )?;
    Ok(())
}

/// `ExecGetUpdateNewTuple(relinfo, planSlot, oldSlot)` — project the subplan
/// output (with OLD-column references resolved from `oldSlot`) through the
/// update "new tuple" projection (`ri_projectNew`), returning the
/// stored-rowtype slot.
pub fn ExecGetUpdateNewTuple<'mcx>(
    estate: &mut EStateData<'mcx>,
    relinfo: RriId,
    plan_slot: SlotId,
    old_slot: Option<SlotId>,
) -> PgResult<SlotId> {
    debug_assert!(estate.result_rel(relinfo).ri_projectNewInfoValid);
    backend_executor_execExpr_seams::exec_project_new_tuple::call(estate, relinfo, plan_slot, old_slot)
}

/// `ExecInitUpdateProjection(mtstate, resultRelInfo)` — build the update
/// projection (`ri_projectNew` + `ri_newTupleSlot`) from this relation's
/// update_colnos list.
pub fn ExecInitUpdateProjection<'mcx>(
    mcx: Mcx<'mcx>,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
) -> PgResult<()> {
    // Usually, mt_lastResultIndex matches the target rel.  If it happens not
    // to, we get the index the hard way.
    let mut whichrel = mtstate.mt_lastResultIndex as usize;
    if mtstate.resultRelInfo.get(whichrel) != Some(&result_rel_info) {
        whichrel = mtstate
            .resultRelInfo
            .iter()
            .position(|&r| r == result_rel_info)
            .expect("ExecInitUpdateProjection: resultRelInfo not in mtstate->resultRelInfo[]");
    }

    let update_colnos: mcx::PgVec<'mcx, i32> = {
        let lists = mtstate
            .mt_updateColnosLists
            .as_ref()
            .expect("ExecInitUpdateProjection: mt_updateColnosLists is NIL");
        let src = &lists[whichrel];
        let mut v = mcx::vec_with_capacity_in(mcx, src.len())?;
        for &c in src.iter() {
            v.push(c);
        }
        v
    };

    // For UPDATE, we use the old tuple to fill up missing values; need two slots
    // matching the table's desired format.
    let rel = relation_alias(estate, result_rel_info);
    let old_tuple_slot = backend_access_table_tableam::table_slot_create(mcx, &rel)?;
    let old_id = estate.make_slot(old_tuple_slot)?;
    let new_tuple_slot = backend_access_table_tableam::table_slot_create(mcx, &rel)?;
    let new_id = estate.make_slot(new_tuple_slot)?;
    estate.result_rel_mut(result_rel_info).ri_oldTupleSlot = Some(old_id);
    estate.result_rel_mut(result_rel_info).ri_newTupleSlot = Some(new_id);

    // need an expression context to do the projection
    if mtstate.ps.ps_ExprContext.is_none() {
        backend_executor_execUtils_seams::exec_assign_expr_context::call(estate, &mut mtstate.ps)?;
    }

    backend_executor_execExpr_seams::exec_build_update_projection::call(
        mtstate,
        estate,
        result_rel_info,
        &update_colnos,
    )?;

    estate.result_rel_mut(result_rel_info).ri_projectNewInfoValid = true;
    Ok(())
}

// ---------------------------------------------------------------------------
// Small field-read helpers over the pooled ResultRelInfo / its relation.
// ---------------------------------------------------------------------------

/// `RelationGetRelid(resultRelInfo->ri_RelationDesc)`.
fn relation_oid(estate: &EStateData<'_>, rri: RriId) -> types_core::Oid {
    estate
        .result_rel(rri)
        .ri_RelationDesc
        .as_ref()
        .map(|r| r.rd_id)
        .unwrap_or(types_core::INVALID_OID)
}

/// `RelationGetRelationName(resultRelInfo->ri_RelationDesc)`.
fn relation_name(estate: &EStateData<'_>, rri: RriId) -> String {
    estate
        .result_rel(rri)
        .ri_RelationDesc
        .as_ref()
        .map(|r| r.rd_rel.relname.as_str().to_owned())
        .unwrap_or_default()
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

/// `RelationGetForm(rel)->relhasindex`.
fn relation_relhasindex(estate: &EStateData<'_>, rri: RriId) -> bool {
    estate
        .result_rel(rri)
        .ri_RelationDesc
        .as_ref()
        .map(|r| r.rd_rel.relhasindex)
        .unwrap_or(false)
}

/// `rel->rd_rel->relispartition`.
fn relation_relispartition(estate: &EStateData<'_>, rri: RriId) -> bool {
    estate
        .result_rel(rri)
        .ri_RelationDesc
        .as_ref()
        .map(|r| r.rd_rel.relispartition)
        .unwrap_or(false)
}

/// `rel->rd_att->constr != NULL`.
fn relation_has_constr(estate: &EStateData<'_>, rri: RriId) -> bool {
    estate
        .result_rel(rri)
        .ri_RelationDesc
        .as_ref()
        .map(|r| r.rd_att.constr.is_some())
        .unwrap_or(false)
}

/// `rel->rd_att->constr && rel->rd_att->constr->has_generated_stored`.
fn relation_has_generated_stored(estate: &EStateData<'_>, rri: RriId) -> bool {
    estate
        .result_rel(rri)
        .ri_RelationDesc
        .as_ref()
        .and_then(|r| r.rd_att.constr.as_ref())
        .map(|c| c.has_generated_stored)
        .unwrap_or(false)
}

/// `((ModifyTable *) mtstate->ps.plan)->onConflictAction == ONCONFLICT_UPDATE`.
fn mtstate_on_conflict_is_update(mtstate: &ModifyTableState<'_>) -> bool {
    mtstate.onConflictAction == types_nodes::modifytable::OnConflictAction::ONCONFLICT_UPDATE
}
