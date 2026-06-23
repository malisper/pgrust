//! The single-tuple INSERT driver (`ExecInsert`), split out of the `insert`
//! family because the C function (~507 lines) is large enough to body-port
//! independently of the rest of the insert path.

use mcx::Mcx;
use types_core::xact::CommandId;
use types_error::{PgError, PgResult};
use nodes::nodes::CmdType;
use nodes::{EStateData, ModifyTableState, RriId, SlotId};
use types_tuple::heaptuple::ItemPointerData;

use crate::lifecycle::{ExecComputeStoredGenerated, ExecPrepareTupleRouting, ExecProcessReturning};
use crate::ModifyTableContext;

/// `WCO_RLS_INSERT_CHECK` (parsenodes.h `WCOKind`).
const WCO_RLS_INSERT_CHECK: i32 = 1;
/// `WCO_RLS_UPDATE_CHECK` (parsenodes.h `WCOKind`).
const WCO_RLS_UPDATE_CHECK: i32 = 2;
/// `WCO_VIEW_CHECK` (parsenodes.h `WCOKind`).
const WCO_VIEW_CHECK: i32 = 0;

// ---------------------------------------------------------------------------
// Seams into unported neighbors that `ExecInsert` reaches. Each is a thin
// marshal+delegate slot the owner installs when it lands; until then a call
// panics with the seam's path (AGENTS.md: loud panic over a silent stub).
// These mirror the trigger/index-AM/table-AM/speculative-lock and the
// root->child tuple-conversion routines this node layer drives across a
// dependency cycle but for which no shared seam declaration exists yet.
// ---------------------------------------------------------------------------

// The BEFORE/INSTEAD-OF/AFTER ROW INSERT trigger entry points are declared in
// `backend-commands-trigger-seams` (the trigger owner) and called via that path
// below — they are no longer locally redeclared here.
use trigger_seams::{
    exec_ar_insert_triggers, exec_br_insert_triggers, exec_ir_insert_triggers,
};

// `ExecCheckIndexConstraints` is owned by execIndexing; its seam is declared in
// `backend-executor-execIndexing-seams` and called through that path below.
use execIndexing_seams::exec_check_index_constraints;

seam_core::seam!(
    /// `GetCurrentTransactionId()` then
    /// `SpeculativeInsertionLockAcquire(xid)` (lmgr.c): acquire this backend's
    /// speculative-insertion lock and return its token (the C
    /// `SpeculativeInsertionLockAcquire` returns the speculative token used to
    /// stamp the tuple). Acquiring the heavyweight lock can `ereport(ERROR)`.
    pub fn speculative_insertion_lock_acquire() -> PgResult<u32>
);

seam_core::seam!(
    /// `SpeculativeInsertionLockRelease(GetCurrentTransactionId())` (lmgr.c):
    /// release this backend's speculative-insertion lock, waking any waiters.
    pub fn speculative_insertion_lock_release() -> PgResult<()>
);

seam_core::seam!(
    /// `table_tuple_insert_speculative(rel, slot, cid, options, bistate,
    /// specToken)` (tableam.h): insert `slot` speculatively, stamped with
    /// `spec_token`. Heap I/O can `ereport(ERROR)`.
    pub fn table_tuple_insert_speculative<'mcx>(
        estate: &mut EStateData<'mcx>,
        result_rel_info: RriId,
        slot: SlotId,
        cid: CommandId,
        options: i32,
        spec_token: u32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `table_tuple_complete_speculative(rel, slot, specToken, succeeded)`
    /// (tableam.h): complete (`succeeded`) or kill (`!succeeded`) a previously
    /// speculatively inserted tuple. Heap I/O can `ereport(ERROR)`.
    pub fn table_tuple_complete_speculative<'mcx>(
        estate: &mut EStateData<'mcx>,
        result_rel_info: RriId,
        slot: SlotId,
        spec_token: u32,
        succeeded: bool,
    ) -> PgResult<()>
);

// The cross-partition UPDATE save-old root→child conversion uses the
// already-installed `exec_get_root_to_child_map` seam from
// `backend-executor-execUtils-seams` (which returns the AttrMap copy) plus
// `execute_attr_map_slot_explicit` from `backend-executor-execTuples-seams`;
// the tableOid/tid carry is done inline on the EState slot. No local seams here.

/// `ExecInsert(context, resultRelInfo, slot, canSetTag, inserted_tuple,
/// insert_destrel)` — insert `slot` into `resultRelInfo`'s relation (or route
/// it to a partition), firing BEFORE/INSTEAD-OF/AFTER triggers, computing
/// generated columns, checking constraints/WCOs, maintaining indexes, and
/// returning the RETURNING projection (or `None` when there is none). On a
/// cross-partition UPDATE the freshly inserted tuple slot and its destination
/// rel are reported back through `inserted_tuple` / `insert_destrel`.
pub fn ExecInsert<'mcx>(
    mcx: Mcx<'mcx>,
    context: &mut ModifyTableContext,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    slot: SlotId,
    can_set_tag: bool,
    mut inserted_tuple: Option<&mut Option<SlotId>>,
    mut insert_destrel: Option<&mut Option<RriId>>,
) -> PgResult<Option<SlotId>> {
    // ModifyTable *node = (ModifyTable *) mtstate->ps.plan;
    // OnConflictAction onconflict = node->onConflictAction;
    let onconflict = mtstate.onConflictAction;
    let plan_slot = context.planSlot;

    // List *recheckIndexes = NIL;
    let mut recheck_indexes: mcx::PgVec<'mcx, types_core::Oid> = mcx::vec_with_capacity_in(mcx, 0)?;
    // TupleTableSlot *result = NULL;
    let mut result: Option<SlotId> = None;

    let mut result_rel_info = result_rel_info;
    let mut slot = slot;

    // If the input result relation is a partitioned table, find the leaf
    // partition to insert the tuple into.
    //
    //   if (proute) {
    //       slot = ExecPrepareTupleRouting(mtstate, estate, proute,
    //                                      resultRelInfo, slot, &partRelInfo);
    //       resultRelInfo = partRelInfo;
    //   }
    if mtstate.mt_partition_tuple_routing.is_some() {
        let mut part_rel_info: Option<RriId> = None;
        // C passes the PartitionTupleRouting pointer alongside mtstate; here we
        // move the owned routing struct out for the &mut and restore it after,
        // since both borrows are needed by the routing helper.
        let mut proute = mtstate
            .mt_partition_tuple_routing
            .take()
            .expect("mt_partition_tuple_routing checked is_some");
        let routed = ExecPrepareTupleRouting(
            mcx,
            mtstate,
            estate,
            &mut proute,
            result_rel_info,
            slot,
            &mut part_rel_info,
        );
        mtstate.mt_partition_tuple_routing = Some(proute);
        slot = routed?;
        result_rel_info = part_rel_info.expect("ExecPrepareTupleRouting set partRelInfo");
    }

    // ExecMaterializeSlot(slot);
    execTuples_seams::exec_materialize_slot::call(estate, slot)?;

    // resultRelationDesc = resultRelInfo->ri_RelationDesc;
    let result_relation_oid = relation_oid(estate, result_rel_info);

    // Open the table's indexes, if we have not done so already, so that we can
    // add new index entries for the inserted tuple.
    //
    //   if (resultRelationDesc->rd_rel->relhasindex &&
    //       resultRelInfo->ri_IndexRelationDescs == NULL)
    //       ExecOpenIndices(resultRelInfo, onconflict != ONCONFLICT_NONE);
    let relhasindex = relation_relhasindex(estate, result_rel_info);
    let indexes_open = estate
        .result_rel(result_rel_info)
        .ri_IndexRelationDescs
        .is_some();
    if relhasindex && !indexes_open {
        execIndexing_seams::exec_open_indices::call(
            estate,
            result_rel_info,
            onconflict != nodes::modifytable::ONCONFLICT_NONE,
        )?;
    }

    // BEFORE ROW INSERT Triggers.
    //
    // Note: We fire BEFORE ROW TRIGGERS for every attempted insertion in an
    // INSERT ... ON CONFLICT statement. We cannot check for constraint
    // violations before firing these triggers, because they can change the
    // values to insert. Also, they can run arbitrary user-defined code with
    // side-effects that we can't cancel by just not inserting the tuple.
    if relation_trig_insert_before_row(estate, result_rel_info) {
        // Flush any pending inserts, so rows are visible to the triggers
        if !estate.es_insert_pending_result_relations.is_empty() {
            crate::insert::ExecPendingInserts(mcx, estate)?;
        }

        if !exec_br_insert_triggers::call(estate, result_rel_info, slot)? {
            return Ok(None); // "do nothing"
        }
    }

    // INSTEAD OF ROW INSERT Triggers
    if relation_trig_insert_instead_row(estate, result_rel_info) {
        if !exec_ir_insert_triggers::call(estate, result_rel_info, slot)? {
            return Ok(None); // "do nothing"
        }
    } else if estate.result_rel(result_rel_info).ri_has_fdw_routine {
        // GENERATED expressions might reference the tableoid column, so
        // (re-)initialize tts_tableOid before evaluating them.
        estate.slot_mut(slot).tts_tableOid = result_relation_oid;

        // Compute stored generated columns
        if relation_has_generated_stored(estate, result_rel_info) {
            ExecComputeStoredGenerated(
                mcx,
                estate,
                result_rel_info,
                slot,
                CmdType::CMD_INSERT,
            )?;
        }

        // If the FDW supports batching, and batching is requested, accumulate
        // rows and insert them in batches. Otherwise use the per-row inserts.
        if relation_batch_size(estate, result_rel_info) > 1 {
            // The batch-buffer path reads the ResultRelInfo's FDW batch fields
            // (ri_Slots / ri_PlanSlots / ri_NumSlots / ri_NumSlotsInitialized /
            // ri_BatchSize) and calls ExecBatchInsert / MakeSingleTupleTableSlot
            // / ExecCopySlot — all owned by the unported FDW batch path whose
            // body is gated in `crate::insert::ExecBatchInsert`. The fields are
            // not carried on the trimmed ResultRelInfo, so reaching the batch
            // accumulation is an unported callee (mirror-PG-and-panic).
            return Err(PgError::error(
                "FDW batch insert (ri_BatchSize > 1) path not yet ported",
            ));
        }

        // insert into foreign table: let the FDW do it
        let fdw_slot = fdw_exec_foreign_insert(estate, result_rel_info, slot, plan_slot)?;

        match fdw_slot {
            None => return Ok(None), // "do nothing"
            Some(s) => {
                slot = s;
                // AFTER ROW Triggers or RETURNING expressions might reference
                // the tableoid column, so (re-)initialize tts_tableOid before
                // evaluating them. (This covers the case where the FDW replaced
                // the slot.)
                estate.slot_mut(slot).tts_tableOid = result_relation_oid;
            }
        }
    } else {
        // Constraints and GENERATED expressions might reference the tableoid
        // column, so (re-)initialize tts_tableOid before evaluating them.
        estate.slot_mut(slot).tts_tableOid = result_relation_oid;

        // Compute stored generated columns
        if relation_has_generated_stored(estate, result_rel_info) {
            ExecComputeStoredGenerated(
                mcx,
                estate,
                result_rel_info,
                slot,
                CmdType::CMD_INSERT,
            )?;
        }

        // Check any RLS WITH CHECK policies.
        //
        // Normally we should check INSERT policies. But if the insert is the
        // result of a partition key update that moved the tuple to a new
        // partition, we should instead check UPDATE policies, because we are
        // executing policies defined on the target table, and not those defined
        // on the child partitions.
        //
        // If we're running MERGE, we refer to the action that we're executing to
        // know if we're doing an INSERT or UPDATE to a partition table.
        let wco_kind = if mtstate.operation == CmdType::CMD_UPDATE {
            WCO_RLS_UPDATE_CHECK
        } else if mtstate.operation == CmdType::CMD_MERGE {
            let merge_cmd = mtstate
                .mt_merge_action
                .as_deref()
                .and_then(|mas| mas.mas_action.as_deref())
                .map(|a| a.commandType)
                .expect("CMD_MERGE INSERT requires mt_merge_action->mas_action");
            if merge_cmd == CmdType::CMD_UPDATE {
                WCO_RLS_UPDATE_CHECK
            } else {
                WCO_RLS_INSERT_CHECK
            }
        } else {
            WCO_RLS_INSERT_CHECK
        };

        // ExecWithCheckOptions() will skip any WCOs which are not of the kind we
        // are looking for at this point.
        if estate.result_rel(result_rel_info).ri_has_with_check_options {
            execMain_seams::exec_with_check_options::call(
                estate,
                wco_kind,
                result_rel_info,
                slot,
            )?;
        }

        // Check the constraints of the tuple.
        if relation_has_constr(estate, result_rel_info) {
            execMain_seams::exec_constraints::call(
                estate,
                result_rel_info,
                slot,
            )?;
        }

        // Also check the tuple against the partition constraint, if there is
        // one; except that if we got here via tuple-routing, we don't need to if
        // there's no BR trigger defined on the partition.
        if relation_relispartition(estate, result_rel_info)
            && (estate.result_rel(result_rel_info).ri_RootResultRelInfo.is_none()
                || relation_trig_insert_before_row(estate, result_rel_info))
        {
            execMain_seams::exec_partition_check::call(
                estate,
                result_rel_info,
                slot,
                true,
            )?;
        }

        if onconflict != nodes::modifytable::ONCONFLICT_NONE
            && estate.result_rel(result_rel_info).ri_NumIndices > 0
        {
            // Perform a speculative insertion.
            //   ItemPointerSetInvalid(&invalidItemPtr);
            //   arbiterIndexes = resultRelInfo->ri_onConflictArbiterIndexes;
            let invalid_item_ptr = item_pointer_invalid();
            let arbiter_indexes: mcx::PgVec<'mcx, types_core::Oid> = {
                let src = estate
                    .result_rel(result_rel_info)
                    .ri_onConflictArbiterIndexes
                    .as_ref();
                let mut v = mcx::vec_with_capacity_in(mcx, src.map(|s| s.len()).unwrap_or(0))?;
                if let Some(src) = src {
                    for &o in src.iter() {
                        v.push(o);
                    }
                }
                v
            };

            // We loop back to `vlock:` if we find a conflict below, either during
            // the pre-check, or when we re-check after inserting the tuple
            // speculatively.
            loop {
                // vlock:
                postgres_seams::check_for_interrupts::call()?;

                let mut spec_conflict = false;
                let mut conflict_tid = ItemPointerData::default();

                if !exec_check_index_constraints::call(
                    estate,
                    result_rel_info,
                    slot,
                    &mut conflict_tid,
                    &invalid_item_ptr,
                    &arbiter_indexes,
                )? {
                    // committed conflict tuple found
                    if onconflict == nodes::modifytable::ONCONFLICT_UPDATE {
                        // In case of ON CONFLICT DO UPDATE, execute the UPDATE
                        // part. Be prepared to retry if the UPDATE fails because
                        // of another concurrent UPDATE/DELETE to the conflict
                        // tuple.
                        let mut returning: Option<SlotId> = None;

                        if crate::insert::ExecOnConflictUpdate(
                            mcx,
                            context,
                            mtstate,
                            estate,
                            result_rel_info,
                            &conflict_tid,
                            slot,
                            can_set_tag,
                            &mut returning,
                        )? {
                            instr_count_tuples2(mtstate, 1.0);
                            return Ok(returning);
                        } else {
                            // goto vlock;
                            continue;
                        }
                    } else {
                        // In case of ON CONFLICT DO NOTHING, do nothing. However,
                        // verify that the tuple is visible to the executor's MVCC
                        // snapshot at higher isolation levels.
                        debug_assert!(onconflict == nodes::modifytable::ONCONFLICT_NOTHING);
                        let temp_slot = execMain_seams::exec_get_returning_slot::call(
                            estate,
                            result_rel_info,
                        )?;
                        crate::insert::ExecCheckTIDVisible(
                            estate,
                            result_rel_info,
                            &conflict_tid,
                            temp_slot,
                        )?;
                        instr_count_tuples2(mtstate, 1.0);
                        return Ok(None);
                    }
                }

                // Before we start insertion proper, acquire our "speculative
                // insertion lock". Others can use that to wait for us to decide
                // if we're going to go ahead with the insertion, instead of
                // waiting for the whole transaction to complete.
                let spec_token = speculative_insertion_lock_acquire::call()?;

                // insert the tuple, with the speculative token
                let cid = estate.es_output_cid;
                table_tuple_insert_speculative::call(
                    estate,
                    result_rel_info,
                    slot,
                    cid,
                    0,
                    spec_token,
                )?;

                // insert index entries for tuple
                recheck_indexes = execIndexing_seams::exec_insert_index_tuples::call(
                    mcx,
                    estate,
                    result_rel_info,
                    slot,
                    false,
                    true,
                    Some(&mut spec_conflict),
                    &arbiter_indexes,
                    false,
                )?;

                // adjust the tuple's state accordingly
                table_tuple_complete_speculative::call(
                    estate,
                    result_rel_info,
                    slot,
                    spec_token,
                    !spec_conflict,
                )?;

                // Wake up anyone waiting for our decision. They will re-check the
                // tuple, see that it's no longer speculative, and wait on our XID
                // as if this was a regularly inserted tuple all along. Or if we
                // killed the tuple, they will see it's dead, and proceed as if the
                // tuple never existed.
                speculative_insertion_lock_release::call()?;

                // If there was a conflict, start from the beginning. We'll do the
                // pre-check again, which will now find the conflicting tuple
                // (unless it aborts before we get there).
                if spec_conflict {
                    // list_free(recheckIndexes); goto vlock;
                    recheck_indexes = mcx::vec_with_capacity_in(mcx, 0)?;
                    let _ = &recheck_indexes;
                    continue;
                }

                // Since there was no insertion conflict, we're done
                break;
            }
        } else {
            // insert the tuple normally
            let cid = estate.es_output_cid;
            let rel = relation_alias(estate, result_rel_info);
            let mcx = estate.es_query_cxt;
            let slot_ref = estate.slot_data_mut(slot);
            table_tableam::table_tuple_insert(mcx, &rel, slot_ref, cid, 0, None)?;

            // insert index entries for tuple
            if estate.result_rel(result_rel_info).ri_NumIndices > 0 {
                recheck_indexes = execIndexing_seams::exec_insert_index_tuples::call(
                    mcx,
                    estate,
                    result_rel_info,
                    slot,
                    false,
                    false,
                    None,
                    &[],
                    false,
                )?;
            }
        }
    }

    if can_set_tag {
        estate.es_processed += 1;
    }

    // If this insert is the result of a partition key update that moved the
    // tuple to a new partition, put this row into the transition NEW TABLE, if
    // there is one. We need to do this separately for DELETE and INSERT because
    // they happen on different tables.
    //
    //   ar_insert_trig_tcs = mtstate->mt_transition_capture;
    let mut fire_with_capture = true;
    let cross_part_new_table = mtstate.operation == CmdType::CMD_UPDATE
        && mtstate
            .mt_transition_capture
            .as_deref()
            .map(|tc| tc.tcs_update_new_table)
            .unwrap_or(false);
    if cross_part_new_table {
        trigger_seams::exec_ar_update_triggers::call(
            estate,
            result_rel_info,
            None,
            None,
            None,
            None,
            Some(slot),
            &[],
            mtstate.mt_transition_capture.as_deref_mut(),
            false,
        )?;

        // We've already captured the NEW TABLE row, so make sure any AR INSERT
        // trigger fired below doesn't capture it again.
        //   ar_insert_trig_tcs = NULL;
        fire_with_capture = false;
    }

    // AFTER ROW INSERT Triggers
    let capture = if fire_with_capture {
        mtstate.mt_transition_capture.as_deref_mut()
    } else {
        None
    };
    exec_ar_insert_triggers::call(
        estate,
        result_rel_info,
        slot,
        &recheck_indexes,
        capture,
    )?;

    // list_free(recheckIndexes);
    drop(recheck_indexes);

    // Check any WITH CHECK OPTION constraints from parent views. We are required
    // to do this after testing all constraints and uniqueness violations per the
    // SQL spec, so we do it after actually inserting the record into the heap and
    // all indexes.
    //
    // ExecWithCheckOptions will elog(ERROR) if a violation is found, so the tuple
    // will never be seen, if it violates the WITH CHECK OPTION.
    //
    // ExecWithCheckOptions() will skip any WCOs which are not of the kind we are
    // looking for at this point.
    if estate.result_rel(result_rel_info).ri_has_with_check_options {
        execMain_seams::exec_with_check_options::call(
            estate,
            WCO_VIEW_CHECK,
            result_rel_info,
            slot,
        )?;
    }

    // Process RETURNING if present
    if estate.result_rel(result_rel_info).ri_has_project_returning {
        let mut old_slot: Option<SlotId> = None;

        // If this is part of a cross-partition UPDATE, and the RETURNING list
        // refers to any OLD columns, ExecDelete() will have saved the tuple
        // deleted from the original partition, which we must use here to compute
        // the OLD column values. Otherwise, all OLD column values will be NULL.
        if let Some(cp_deleted) = context.cpDeletedSlot {
            // Convert the OLD tuple to the new partition's format/slot, if
            // needed. Note that ExecDelete() already converted it to the root's
            // partition's format/slot.
            old_slot = Some(cp_deleted);
            // tupconv_map = ExecGetRootToChildMap(resultRelInfo, estate);
            // if (tupconv_map != NULL) { oldSlot = execute_attr_map_slot(...,
            //   ExecGetReturningSlot(estate, resultRelInfo)); ... }
            if let Some(attr_map) =
                execUtils_seams::exec_get_root_to_child_map::call(
                    mcx,
                    estate,
                    result_rel_info,
                )?
            {
                let returning_slot = execMain_seams::exec_get_returning_slot::call(
                    estate,
                    result_rel_info,
                )?;
                let converted =
                    execTuples_seams::execute_attr_map_slot_explicit::call(
                        estate,
                        &attr_map,
                        cp_deleted,
                        returning_slot,
                    )?;

                // oldSlot->tts_tableOid = context->cpDeletedSlot->tts_tableOid;
                // ItemPointerCopy(&context->cpDeletedSlot->tts_tid, &oldSlot->tts_tid);
                let (src_oid, src_tid) = {
                    let s = estate.slot(cp_deleted);
                    (s.tts_tableOid, s.tts_tid)
                };
                let dst = estate.slot_mut(converted);
                dst.tts_tableOid = src_oid;
                dst.tts_tid = src_tid;
                old_slot = Some(converted);
            }
        }

        result = Some(ExecProcessReturning(
            estate,
            result_rel_info,
            CmdType::CMD_INSERT,
            old_slot,
            Some(slot),
            plan_slot,
        )?);

        // For a cross-partition UPDATE, release the old tuple, first making sure
        // that the result slot has a local copy of any pass-by-reference values.
        if let Some(cp_deleted) = context.cpDeletedSlot {
            execTuples_seams::exec_materialize_slot::call(
                estate,
                result.expect("RETURNING produced a slot"),
            )?;
            let old = old_slot.expect("cross-partition RETURNING old_slot set");
            exec_clear_tuple(estate, old)?;
            if cp_deleted != old {
                exec_clear_tuple(estate, cp_deleted)?;
            }
            context.cpDeletedSlot = None;
        }
    }

    if let Some(it) = inserted_tuple.as_deref_mut() {
        *it = Some(slot);
    }
    if let Some(idr) = insert_destrel.as_deref_mut() {
        *idr = Some(result_rel_info);
    }

    Ok(result)
}

/// `slot->ri_FdwRoutine->ExecForeignInsert(estate, relinfo, slot, planSlot)`
/// (fdwapi.h): let the FDW perform the insert. Returns the slot the FDW filled,
/// or `None` ("do nothing"). The FDW vtable lands with the fdwapi type; until
/// then reaching a foreign-table insert is an unported callee
/// (mirror-PG-and-panic).
fn fdw_exec_foreign_insert<'mcx>(
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    slot: SlotId,
    plan_slot: Option<SlotId>,
) -> PgResult<Option<SlotId>> {
    let _ = (estate, result_rel_info, slot, plan_slot);
    panic!(
        "ExecInsert: ri_FdwRoutine->ExecForeignInsert (fdwapi vtable) not yet ported"
    );
}

/// `ExecClearTuple(slot)` (execTuples.c): clear the slot's tuple.
fn exec_clear_tuple<'mcx>(estate: &mut EStateData<'mcx>, slot: SlotId) -> PgResult<()> {
    execTuples_seams::exec_clear_tuple::call(estate, slot)
}

/// `InstrCountTuples2(&mtstate->ps, delta)` (execnodes.h): bump the node's
/// `instrument->ntuples2` counter, if instrumentation is enabled.
fn instr_count_tuples2(mtstate: &mut ModifyTableState<'_>, delta: f64) {
    if let Some(instr) = mtstate.ps.instrument.as_deref_mut() {
        instr.ntuples2 += delta;
    }
}

/// `ItemPointerSetInvalid(&p)` (itemptr.h): an invalid item pointer
/// (`InvalidBlockNumber` / `InvalidOffsetNumber`).
fn item_pointer_invalid() -> ItemPointerData {
    ItemPointerData::new(types_core::primitive::InvalidBlockNumber, 0)
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

/// An `alias()` of `ri_RelationDesc` (shared, no release authority).
fn relation_alias<'mcx>(estate: &EStateData<'mcx>, rri: RriId) -> rel::Relation<'mcx> {
    estate
        .result_rel(rri)
        .ri_RelationDesc
        .as_ref()
        .expect("ResultRelInfo has no relation")
        .alias()
}

/// `resultRelationDesc->rd_rel->relhasindex`.
fn relation_relhasindex(estate: &EStateData<'_>, rri: RriId) -> bool {
    estate
        .result_rel(rri)
        .ri_RelationDesc
        .as_ref()
        .map(|r| r.rd_rel.relhasindex)
        .unwrap_or(false)
}

/// `resultRelationDesc->rd_rel->relispartition`.
fn relation_relispartition(estate: &EStateData<'_>, rri: RriId) -> bool {
    estate
        .result_rel(rri)
        .ri_RelationDesc
        .as_ref()
        .map(|r| r.rd_rel.relispartition)
        .unwrap_or(false)
}

/// `resultRelationDesc->rd_att->constr != NULL`.
fn relation_has_constr(estate: &EStateData<'_>, rri: RriId) -> bool {
    estate
        .result_rel(rri)
        .ri_RelationDesc
        .as_ref()
        .map(|r| r.rd_att.constr.is_some())
        .unwrap_or(false)
}

/// `resultRelationDesc->rd_att->constr && rd_att->constr->has_generated_stored`.
fn relation_has_generated_stored(estate: &EStateData<'_>, rri: RriId) -> bool {
    estate
        .result_rel(rri)
        .ri_RelationDesc
        .as_ref()
        .and_then(|r| r.rd_att.constr.as_ref())
        .map(|c| c.has_generated_stored)
        .unwrap_or(false)
}

/// `resultRelInfo->ri_TrigDesc && ri_TrigDesc->trig_insert_before_row`.
fn relation_trig_insert_before_row(estate: &EStateData<'_>, rri: RriId) -> bool {
    estate
        .result_rel(rri)
        .ri_TrigDesc
        .as_deref()
        .map(|t| t.trig_insert_before_row)
        .unwrap_or(false)
}

/// `resultRelInfo->ri_TrigDesc && ri_TrigDesc->trig_insert_instead_row`.
fn relation_trig_insert_instead_row(estate: &EStateData<'_>, rri: RriId) -> bool {
    estate
        .result_rel(rri)
        .ri_TrigDesc
        .as_deref()
        .map(|t| t.trig_insert_instead_row)
        .unwrap_or(false)
}

/// `resultRelInfo->ri_BatchSize`.
fn relation_batch_size(estate: &EStateData<'_>, rri: RriId) -> i32 {
    let _ = (estate, rri);
    // ri_BatchSize is a FDW-batch field not carried on the trimmed
    // ResultRelInfo; an FDW with batching enabled (>1) reaches the unported
    // batch path. The default (no batching) is 1.
    1
}
