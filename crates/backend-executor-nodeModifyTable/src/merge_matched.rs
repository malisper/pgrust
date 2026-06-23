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
use types_tuple::backend_access_common_heaptuple::FormedTuple;
use types_tuple::heaptuple::ItemPointerData;

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

/// `MovedPartitionsBlockNumber` (`storage/itemptr.h`) — `InvalidBlockNumber`.
const MOVED_PARTITIONS_BLOCK_NUMBER: BlockNumber = InvalidBlockNumber;
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
// In-crate reads of the now-owned per-action MERGE exec state. The fields
// `ResultRelInfo.ri_MergeActions[]`, `ri_MergeJoinCondition`, `ri_RowIdAttNo`,
// `ri_projectReturning`, and `ri_WithCheckOptions` are carried on the shared
// `types_nodes::ResultRelInfo` vocabulary (built by `ExecInitMerge` /
// `ExecInitPartitionInfo`), so this unit reads them directly — exactly as
// `ExecMergeNotMatched` reads `ri_MergeActions[MERGE_WHEN_NOT_MATCHED_BY_TARGET]`.
// Only the genuinely foreign action-loop primitives (`ExecQual`, `ExecProject`,
// `ExecGetJunkAttribute`/`slot_getattr`, `ExecForceStoreHeapTuple`,
// `InstrUpdateTupleCount`) cross into their real owner `-seams` crates
// (execExpr / execTuples / instrument).
// ---------------------------------------------------------------------------

/// `resultRelInfo->ri_MergeActions[MERGE_WHEN_MATCHED] == NIL &&
/// resultRelInfo->ri_MergeActions[MERGE_WHEN_NOT_MATCHED_BY_SOURCE] == NIL`
/// (execnodes.h): are both the WHEN MATCHED and WHEN NOT MATCHED BY SOURCE
/// action lists empty for this result relation?
fn ri_merge_matched_actions_empty(estate: &EStateData<'_>, rri: RriId) -> bool {
    merge_actions_count(estate, rri, MERGE_WHEN_MATCHED) == 0
        && merge_actions_count(estate, rri, MERGE_WHEN_NOT_MATCHED_BY_SOURCE) == 0
}

/// `list_length(resultRelInfo->ri_MergeActions[kind])` (execnodes.h).
fn merge_actions_count(estate: &EStateData<'_>, rri: RriId, kind: MergeMatchKind) -> usize {
    estate.result_rel(rri).ri_MergeActions[kind as usize]
        .as_ref()
        .map(|l| l.len())
        .unwrap_or(0)
}

/// `ExecQual(resultRelInfo->ri_MergeJoinCondition, econtext)` (executor.h):
/// evaluate this result relation's MERGE join condition over the node's
/// expression context. A `NULL` condition is always-true (see
/// `transform_MERGE_to_join`); the compiled `ExprState` is owned on the shared
/// `ResultRelInfo`, so it is cloned out and evaluated through the real execExpr
/// owner seam. Fallible on `ereport(ERROR)`.
fn eval_merge_join_condition<'mcx>(
    estate: &mut EStateData<'mcx>,
    rri: RriId,
    econtext: types_nodes::EcxtId,
) -> PgResult<bool> {
    // The compiled join condition `ExprState` lives on the pooled `ResultRelInfo`
    // and cannot be borrowed `&mut` while `estate` is also borrowed `&mut`, nor
    // `.clone()`d (a compiled `ExprState` carries a context-allocated step program
    // with no copyObject). So MOVE it out (leaving `None`), evaluate it through the
    // execExpr owner seam, and restore it before returning — faithfully mirroring
    // C aliasing `resultRelInfo->ri_MergeJoinCondition` into `ExecQual`. A `NULL`
    // condition is always-true (transform_MERGE_to_join).
    let mut cond = estate.result_rel_mut(rri).ri_MergeJoinCondition.take();
    let result = match cond.as_mut() {
        Some(state) => backend_executor_execExpr_seams::exec_qual::call(state, econtext, estate),
        None => Ok(true),
    };
    estate.result_rel_mut(rri).ri_MergeJoinCondition = cond;
    result
}

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
    oldtuple: Option<FormedTuple<'mcx>>,
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
    if ri_merge_matched_actions_empty(estate, result_rel_info) {
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

    if let Some(tuple) = oldtuple.as_ref() {
        debug_assert!(!need_lock_tag_tuple);
        let mcx = estate.es_query_cxt;
        let formed = tuple.clone_in(mcx)?;
        backend_executor_execTuples_seams::exec_force_store_formed_heap_tuple::call(
            estate,
            old_tuple_slot,
            formed,
            false,
        )?;
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
        let mcx = estate.es_query_cxt;
        let slot_ref = estate.slot_data_mut(old_tuple_slot);
        // if (!table_tuple_fetch_row_version(resultRelInfo->ri_RelationDesc,
        //                                    tupleid, SnapshotAny,
        //                                    resultRelInfo->ri_oldTupleSlot))
        //     elog(ERROR, "failed to fetch the target tuple");
        if !backend_access_table_tableam::table_tuple_fetch_row_version(
            mcx, &rel, &tid, &any, slot_ref,
        )? {
            return Err(PgError::error("failed to fetch the target tuple"));
        }
    }

    // Test the join condition.  If it's satisfied, perform a MATCHED action.
    // Otherwise, perform a NOT MATCHED BY SOURCE action.
    //
    // Note that this join condition will be NULL if there are no NOT MATCHED BY
    // SOURCE actions --- see transform_MERGE_to_join(). In that case, we need
    // only consider MATCHED actions here.
    let mut action_kind = if eval_merge_join_condition(estate, result_rel_info, econtext)? {
        MERGE_WHEN_MATCHED
    } else {
        MERGE_WHEN_NOT_MATCHED_BY_SOURCE
    };

    // lmerge_matched: loop label; the C "goto lmerge_matched" restarts the
    // action search after a concurrent update.
    'lmerge_matched: loop {
        let n_actions = merge_actions_count(estate, result_rel_info, action_kind);

        let mut idx = 0;
        while idx < n_actions {
            // MergeActionState *relaction = (MergeActionState *) lfirst(l);
            // CmdType commandType = relaction->mas_action->commandType;
            //
            // Snapshot the per-action scalars and MOVE the compiled
            // `mas_whenqual` / `mas_proj` out of the pooled
            // `ri_MergeActions[action_kind][idx]` (leaving `None`). A compiled
            // `ExprState` / `ProjectionInfo` cannot be `.clone()`d (the step
            // program is context-allocated; the derived clone is a loud guard)
            // and cannot be borrowed `&mut` while `estate` is borrowed `&mut`.
            // We restore them into the pool as soon as the qual test and (for
            // UPDATE) the projection are done, so a re-entry / EvalPlanQual retry
            // finds them. Faithful to C aliasing the same ExprState.
            let (command_type, action_match_kind, action_overriding, mut whenqual, mut proj) = {
                let action = &mut estate.result_rel_mut(result_rel_info).ri_MergeActions
                    [action_kind as usize]
                    .as_mut()
                    .expect("matched MERGE action list present")[idx];
                let mas_action = action
                    .mas_action
                    .as_ref()
                    .expect("MergeActionState has a MergeAction");
                let command_type = mas_action.commandType;
                let action_match_kind = mas_action.matchKind;
                let action_overriding = mas_action.overriding;
                let whenqual = action.mas_whenqual.take();
                let proj = action.mas_proj.take();
                (command_type, action_match_kind, action_overriding, whenqual, proj)
            };

            // Restore the moved-out compiled states into the pooled action.
            macro_rules! restore_matched_action_states {
                () => {{
                    let action = &mut estate.result_rel_mut(result_rel_info).ri_MergeActions
                        [action_kind as usize]
                        .as_mut()
                        .expect("matched MERGE action list present")[idx];
                    action.mas_whenqual = whenqual.take();
                    action.mas_proj = proj.take();
                }};
            }
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
            //   if (!ExecQual(relaction->mas_whenqual, econtext)) continue;
            let passed = match whenqual.as_mut() {
                Some(state) => {
                    backend_executor_execExpr_seams::exec_qual::call(state, econtext, estate)?
                }
                None => true,
            };
            if !passed {
                restore_matched_action_states!();
                idx += 1;
                continue;
            }

            // Check if the existing target tuple meets the USING checks of
            // UPDATE/DELETE RLS policies. If those checks fail, we throw an
            // error. The WITH CHECK quals for UPDATE RLS policies are applied
            // in ExecUpdateAct(). We must do this after WHEN quals are
            // evaluated, so that we check policies only when they matter.
            if estate.result_rel(result_rel_info).ri_WithCheckOptions.is_some()
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
                    //   newslot = ExecProject(relaction->mas_proj);
                    let proj = proj
                        .as_mut()
                        .expect("CMD_UPDATE MERGE action has a projection");
                    let projected =
                        backend_executor_execExpr_seams::exec_project_info::call(proj, estate)?;
                    newslot = Some(projected);

                    // whenqual + proj are no longer needed this iteration; put
                    // them back so an EvalPlanQual retry re-entry finds them.
                    restore_matched_action_states!();

                    // mtstate->mt_merge_action = relaction;
                    //
                    // The C aliases the active pooled `MergeActionState` so the
                    // executor can attribute the running WHEN clause (consumed by
                    // ExecInsert's RLS WCO-kind selection, EXPLAIN, and error
                    // context). The owned-by-value tree can't share the `&'mcx`
                    // borrow into the pooled state, so we materialize an owned
                    // `MergeActionState` carrying the active action's identity —
                    // exactly the fields any consumer reads (`mas_action`'s
                    // commandType/matchKind/overriding), mirroring how
                    // ExecInitMerge builds the pooled state.
                    mtstate.mt_merge_action = Some(mcx::alloc_in(
                        mcx,
                        types_nodes::modifytable::MergeActionState {
                            type_: types_nodes::nodes::T_MergeActionState,
                            mas_action: Some(mcx::alloc_in(
                                mcx,
                                types_nodes::modifytable::MergeAction {
                                    matchKind: action_match_kind,
                                    commandType: command_type,
                                    overriding: action_overriding,
                                    qual: None,
                                    targetList: None,
                                    updateColnos: None,
                                },
                            )?),
                            mas_proj: None,
                            mas_whenqual: None,
                        },
                    )?);

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
                    // whenqual + proj are no longer needed this iteration.
                    restore_matched_action_states!();
                    // mtstate->mt_merge_action = relaction; (see the CMD_UPDATE
                    // arm — materialize an owned MergeActionState for the active
                    // action so consumers attribute the running WHEN clause.)
                    mtstate.mt_merge_action = Some(mcx::alloc_in(
                        mcx,
                        types_nodes::modifytable::MergeActionState {
                            type_: types_nodes::nodes::T_MergeActionState,
                            mas_action: Some(mcx::alloc_in(
                                mcx,
                                types_nodes::modifytable::MergeAction {
                                    matchKind: action_match_kind,
                                    commandType: command_type,
                                    overriding: action_overriding,
                                    qual: None,
                                    targetList: None,
                                    updateColnos: None,
                                },
                            )?),
                            mas_proj: None,
                            mas_whenqual: None,
                        },
                    )?);

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
                    //   if (resultRelInfo->ri_TrigDesc &&
                    //       resultRelInfo->ri_TrigDesc->trig_delete_instead_row)
                    let has_instead_delete_row = estate
                        .result_rel(result_rel_info)
                        .ri_TrigDesc
                        .as_ref()
                        .map(|td| td.trig_delete_instead_row)
                        .unwrap_or(false);
                    if has_instead_delete_row {
                        if !backend_commands_trigger_seams::exec_ir_delete_triggers::call(
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
                    // whenqual + proj are no longer needed this iteration.
                    restore_matched_action_states!();
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
                    // transaction. The full MERGE EvalPlanQual recheck (re-fetch
                    // the latest version, recheck the join qual, possibly switch
                    // MATCHED → NOT MATCHED BY SOURCE, then retry the action on
                    // the *new* version) needs the target `tupleid` to advance to
                    // the locked version through the whole `lmerge_matched` retry
                    // — a threading the owned MERGE driver does not yet do, so the
                    // retry would re-lock the old TID and loop forever. Until the
                    // MERGE EPQ tid-advance leg lands, raise a clean serialization
                    // error rather than spin (the plain UPDATE/DELETE EPQ paths
                    // are complete; this only affects concurrent MERGE).
                    if true {
                        return Err(PgError::error(
                            "could not serialize access due to concurrent update",
                        )
                        .with_sqlstate(ERRCODE_T_R_SERIALIZATION_FAILURE));
                    }
                    //   was_matched = relaction->mas_action->matchKind == MERGE_WHEN_MATCHED;
                    let was_matched = action_match_kind == MERGE_WHEN_MATCHED;
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
                    let mcx = estate.es_query_cxt;
                    let inslot = estate.slot_data_mut(inputslot);
                    let lock_result = backend_access_table_tableam::table_tuple_lock(
                        mcx,
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
                                //   (void) ExecGetJunkAttribute(epqslot,
                                //       resultRelInfo->ri_RowIdAttNo, &isNull);
                                let row_id_att = estate.result_rel(result_rel_info).ri_RowIdAttNo;
                                let junk = backend_executor_execTuples_seams::slot_getattr_by_id::call(
                                    estate, epqslot, row_id_att,
                                )?;
                                if junk.isnull {
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
                                let mcx = estate.es_query_cxt;
                                let oldslot_ref = estate.slot_data_mut(old_tuple_slot);
                                if !backend_access_table_tableam::table_tuple_fetch_row_version(
                                    mcx, &rel2, &tid, &any, oldslot_ref,
                                )? {
                                    return Err(PgError::error(
                                        "failed to fetch the target tuple",
                                    ));
                                }

                                if *matched {
                                    *matched =
                                        eval_merge_join_condition(estate, result_rel_info, econtext)?;
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
                                    //
                                    //   if (outerPlanState(mtstate)->instrument &&
                                    //       mergeActions[MERGE_WHEN_NOT_MATCHED_BY_SOURCE] &&
                                    //       mergeActions[MERGE_WHEN_NOT_MATCHED_BY_TARGET])
                                    //       InstrUpdateTupleCount(
                                    //           outerPlanState(mtstate)->instrument, 1.0);
                                    let have_both_not_matched = merge_actions_count(
                                        estate,
                                        result_rel_info,
                                        MERGE_WHEN_NOT_MATCHED_BY_SOURCE,
                                    ) > 0
                                        && merge_actions_count(
                                            estate,
                                            result_rel_info,
                                            MERGE_WHEN_NOT_MATCHED_BY_TARGET,
                                        ) > 0;
                                    if have_both_not_matched {
                                        if let Some(outer) = mtstate.ps.lefttree.as_mut() {
                                            if let Some(instr) =
                                                outer.ps_head_mut().instrument.as_mut()
                                            {
                                                backend_executor_instrument_seams::instr_update_tuple_count::call(
                                                    instr, 1.0,
                                                );
                                            }
                                        }
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
            //   if (resultRelInfo->ri_projectReturning)
            if estate.result_rel(result_rel_info).ri_projectReturning.is_some() {
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
