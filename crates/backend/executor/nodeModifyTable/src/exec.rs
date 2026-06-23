//! The ModifyTable node `ExecProcNode` callback (`ExecModifyTable`), split out
//! of the `lifecycle` family because the C function (~420 lines) is large
//! enough to body-port independently of the rest of the node lifecycle.

use ::mcx::Mcx;
use ::types_error::{PgError, PgResult};
use ::nodes::nodes::CmdType;
use ::nodes::{EStateData, ModifyTableState, RriId, SlotId};
use ::types_tuple::access::{
    RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
};
use ::types_tuple::heaptuple::{Datum, FormedTuple, SlotAttr};
use ::types_tuple::heaptuple::ItemPointerData;

use crate::lifecycle::{ExecLookupResultRelByOid, ExecProcessReturning};
use crate::{
    delete_exec, insert, insert_exec, lifecycle, merge, update, ModifyTableContext,
};

/// `InvalidOid` (postgres_ext.h).
const INVALID_OID: types_core::Oid = types_core::INVALID_OID;
/// `InplaceUpdateTupleLock` == `ExclusiveLock` (lockdefs.h). `LOCKMODE` is `i32`.
const INPLACE_UPDATE_TUPLE_LOCK: i32 = 7;

// ===========================================================================
// Seams onto owners of state the trimmed node model does not yet carry. These
// are the only pieces of `ExecModifyTable` that cannot be evaluated locally:
// reading a junk Datum out of the plan slot (the slot payload is owned by the
// execTuples/execJunk slot model), the `ri_RowIdAttNo`/`ri_usesFdwDirectModify`
// fields not yet on the trimmed `ResultRelInfo` (owned by execMain), and the
// `DatumGetHeapTupleHeader`/`ExecForceStoreHeapTuple` reconstruction of the
// wholerow old tuple (owned by the heaptuple/execTuples model). Each delegates
// to exactly one owner function and panics with its path until that owner
// lands; the surrounding control flow is the C function's, ported in full.
// ===========================================================================

seam_core::seam!(
    /// `ExecGetJunkAttribute(slot, attno, &isNull)` (execJunk.h): read the
    /// Datum of the junk attribute `attno` from `slot`, returning the value and
    /// its null flag. The slot payload is owned by the execTuples slot model.
    ///
    /// The result is the canonical
    /// [`::types_tuple::heaptuple::SlotAttr`], so a
    /// by-reference value (e.g. the 6-byte `ctid` `ItemPointerData` image) crosses
    /// intact as the `ByRef` arm — never collapsed onto a scalar word. The OID
    /// (`mt_resultOidAttno`) consumer reads `value.as_oid()` off the `ByVal` arm.
    pub fn exec_get_junk_attribute<'mcx>(
        estate: &mut EStateData<'mcx>,
        slot: SlotId,
        attno: i32,
    ) -> PgResult<SlotAttr<'mcx>>
);

seam_core::seam!(
    /// `resultRelInfo->ri_RowIdAttNo` (execnodes.h): the resno of the row-ID
    /// junk attribute (ctid or wholerow), set up in `ExecInitModifyTable`. Not
    /// yet carried on the trimmed `ResultRelInfo` (owned by execMain).
    pub fn ri_row_id_attno(estate: &EStateData<'_>, result_rel_info: RriId) -> i32
);

seam_core::seam!(
    /// `resultRelInfo->ri_usesFdwDirectModify` (execnodes.h): the relation is a
    /// foreign table whose modifications the FDW performs directly, so the node
    /// only computes RETURNING. Not yet carried on the trimmed `ResultRelInfo`
    /// (lands with the fdwapi/execMain owner).
    pub fn ri_uses_fdw_direct_modify(estate: &EStateData<'_>, result_rel_info: RriId) -> bool
);

seam_core::seam!(
    /// `(ItemPointer) DatumGetPointer(datum)` then `tuple_ctid = *tupleid`
    /// (htup_details.h / itemptr.h): recover the ctid `ItemPointerData` a ctid
    /// junk Datum points at. The Datum payload model is owned by the
    /// execTuples/heaptuple slot model. The ctid arrives as the canonical
    /// `Datum::ByRef` 6-byte `ItemPointerData` image (`PointerGetDatum(&t_self)`).
    pub fn datum_get_item_pointer<'mcx>(datum: &Datum<'mcx>) -> ItemPointerData
);

seam_core::seam!(
    /// `DatumGetHeapTupleHeader(datum)` then the
    /// `oldtupdata.t_data/t_len/t_self/t_tableOid` assembly (htup_details.h):
    /// reconstruct a tuple from a wholerow junk Datum, with `t_tableOid` set to
    /// `tableoid` (`InvalidOid` for a view) and `t_self` invalid. The carrier is
    /// the data-bearing [`FormedTuple`](::types_tuple::heaptuple::FormedTuple)
    /// (header + user-data area) — a bare `HeapTupleData` header would drop the
    /// column data the wholerow Datum carries, which the downstream
    /// `ExecForceStoreHeapTuple` deform and the FDW/view trigger paths read. The
    /// varlena detoast + header decode is owned by the heaptuple model.
    pub fn datum_get_wholerow_heap_tuple<'mcx>(
        mcx: Mcx<'mcx>,
        datum: &Datum<'mcx>,
        tableoid: types_core::Oid,
    ) -> PgResult<FormedTuple<'mcx>>
);

/// The `PlanState.ExecProcNode` callback installed by `ExecInitModifyTable`
/// (C: `mtstate->ps.ExecProcNode = ExecModifyTable`): `castNode(ModifyTableState,
/// pstate)` then run [`ExecModifyTable`]. Adapts the [`ExecProcNodeMtd`]
/// signature (which carries no `mcx`) by sourcing the per-query context from the
/// EState.
pub fn exec_modify_table_node<'mcx>(
    pstate: &mut ::nodes::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let mcx = estate.es_query_cxt;
    let node = match pstate {
        ::nodes::PlanStateNode::ModifyTable(node) => node,
        other => panic!("castNode(ModifyTableState, pstate) failed: {other:?}"),
    };
    ExecModifyTable(mcx, node, estate)
}

/// `ExecModifyTable(pstate)` — the node's `ExecProcNode` callback: pull tuples
/// from the subplan and apply the INSERT/UPDATE/DELETE/MERGE until done,
/// returning each RETURNING tuple (or `None` at end of execution).
pub fn ExecModifyTable<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let operation = node.operation;

    // CHECK_FOR_INTERRUPTS();
    postgres_seams::check_for_interrupts::call()?;

    // This should NOT get called during EvalPlanQual; we should have passed a
    // subplan tree to EvalPlanQual, instead.  Use a runtime test not just
    // Assert because this condition is easy to miss in testing.
    //
    //   if (estate->es_epq_active != NULL)
    //       elog(ERROR, "ModifyTable should not be called during EvalPlanQual");
    if estate.es_epq_active.is_some() {
        return Err(PgError::error(
            "ModifyTable should not be called during EvalPlanQual",
        ));
    }

    // If we've already completed processing, don't try to do more.  We need
    // this test because ExecPostprocessPlan might call us an extra time.
    if node.mt_done {
        return Ok(None);
    }

    // On first call, fire BEFORE STATEMENT triggers before proceeding.
    if node.fireBSTriggers {
        lifecycle::fireBSTriggers(node, estate)?;
        node.fireBSTriggers = false;
    }

    // Preload local variables.
    //   resultRelInfo = node->resultRelInfo + node->mt_lastResultIndex;
    let mut result_rel_info: RriId = node.resultRelInfo[node.mt_lastResultIndex as usize];
    //   subplanstate = outerPlanState(node);
    // outerPlanState(node) == node->ps.lefttree; the subplan state tree lives in
    // `node.ps.lefttree`. It is dispatched through the execProcnode seam.

    // Set global context. In the owned model `mtstate`/`epqstate`/`estate` are
    // threaded as explicit references to the call sites, so the context carries
    // only the owned per-operation values.
    let mut context = ModifyTableContext {
        planSlot: None,
        tmfd: Default::default(),
        cpDeletedSlot: None,
        cpUpdateReturningSlot: None,
    };

    // Fetch rows from subplan, and execute the required table modification
    // for each row.
    loop {
        // Reset the per-output-tuple exprcontext.  This is needed because
        // triggers expect to use that context as workspace.
        //   ResetPerTupleExprContext(estate);
        reset_per_tuple_expr_context(estate);

        // Reset per-tuple memory context used for processing on conflict and
        // returning clauses, to free any expression evaluation storage
        // allocated in the previous cycle.
        //   if (pstate->ps_ExprContext)
        //       ResetExprContext(pstate->ps_ExprContext);
        if let Some(ecxt) = node.ps.ps_ExprContext {
            reset_expr_context(estate, ecxt);
        }

        // If there is a pending MERGE ... WHEN NOT MATCHED [BY TARGET] action
        // to execute, do so now --- see the comments in ExecMerge().
        if let Some(pending) = node.mt_merge_pending_not_matched {
            context.planSlot = Some(pending);
            context.cpDeletedSlot = None;

            let toprel = node.resultRelInfo[0];
            let slot = merge::ExecMergeNotMatched(
                mcx,
                &mut context,
                node,
                estate,
                toprel,
                node.canSetTag,
            )?;

            // Clear the pending action.
            node.mt_merge_pending_not_matched = None;

            // If we got a RETURNING result, return it to the caller.  We'll
            // continue the work on next call.
            if slot.is_some() {
                return Ok(slot);
            }

            continue; // continue with the next tuple
        }

        // Fetch the next row from subplan.
        //   context.planSlot = ExecProcNode(subplanstate);
        let plan_slot = {
            let subplanstate = node
                .ps
                .lefttree
                .as_mut()
                .expect("outerPlanState(ModifyTable) is NULL");
            execProcnode_seams::exec_proc_node::call(subplanstate, estate)?
        };
        context.planSlot = plan_slot;
        context.cpDeletedSlot = None;

        // No more tuples to process?  TupIsNull(context.planSlot)
        let plan_slot = match context.planSlot {
            Some(id) if !estate.slot(id).is_empty() => id,
            _ => break,
        };

        // When there are multiple result relations, each tuple contains a
        // junk column that gives the OID of the rel from which it came.
        // Extract it and select the correct result relation.
        if attribute_number_is_valid(node.mt_resultOidAttno) {
            // datum = ExecGetJunkAttribute(context.planSlot,
            //                              node->mt_resultOidAttno, &isNull);
            let attr = exec_get_junk_attribute::call(estate, plan_slot, node.mt_resultOidAttno)?;
            let is_null = attr.isnull;
            if is_null {
                // For commands other than MERGE, any tuples having InvalidOid
                // for tableoid are errors.  For MERGE, we may need to handle
                // them as WHEN NOT MATCHED clauses if any, so do that.
                //
                // Note that we use the node's toplevel resultRelInfo, not any
                // specific partition's.
                if operation == CmdType::CMD_MERGE {
                    eval_plan_qual_set_slot(&mut node.mt_epqstate, plan_slot);

                    let toprel = node.resultRelInfo[0];
                    let slot = merge::ExecMerge(
                        mcx,
                        &mut context,
                        node,
                        estate,
                        toprel,
                        None,
                        None,
                        node.canSetTag,
                    )?;

                    // If we got a RETURNING result, return it to the caller.
                    // We'll continue the work on next call.
                    if slot.is_some() {
                        return Ok(slot);
                    }

                    continue; // continue with the next tuple
                }

                return Err(PgError::error("tableoid is NULL"));
            }
            // resultoid = DatumGetObjectId(datum);
            let resultoid = attr.value.as_oid();

            // If it's not the same as last time, we need to locate the rel.
            if resultoid != node.mt_lastResultOid {
                result_rel_info = ExecLookupResultRelByOid(node, estate, resultoid, false, true)?
                    .expect("ExecLookupResultRelByOid(missing_ok=false) returned None");
            }
        }

        // If resultRelInfo->ri_usesFdwDirectModify is true, all we need to do
        // here is compute the RETURNING expressions.
        if ri_uses_fdw_direct_modify::call(estate, result_rel_info) {
            // Assert(resultRelInfo->ri_projectReturning);
            //
            // A scan slot containing the data that was actually inserted,
            // updated or deleted has already been made available to
            // ExecProcessReturning by IterateDirectModify, so no need to
            // provide it here.  The individual old and new slots are not
            // needed, since direct-modify is disabled if the RETURNING list
            // refers to OLD/NEW values.
            let slot = ExecProcessReturning(
                estate,
                result_rel_info,
                operation,
                None,
                None,
                Some(plan_slot),
            )?;

            return Ok(Some(slot));
        }

        // EvalPlanQualSetSlot(&node->mt_epqstate, context.planSlot);
        eval_plan_qual_set_slot(&mut node.mt_epqstate, plan_slot);
        let mut slot: Option<SlotId> = Some(plan_slot);

        let mut tuple_ctid = ItemPointerData::default();
        let mut tupleid: Option<&ItemPointerData> = None;
        let mut oldtuple: Option<FormedTuple<'mcx>> = None;

        // For UPDATE/DELETE/MERGE, fetch the row identity info for the tuple to
        // be updated/deleted/merged.  For a heap relation, that's a TID;
        // otherwise we may have a wholerow junk attr that carries the old tuple
        // in toto.  Keep this in step with the part of ExecInitModifyTable that
        // sets up ri_RowIdAttNo.
        if operation == CmdType::CMD_UPDATE
            || operation == CmdType::CMD_DELETE
            || operation == CmdType::CMD_MERGE
        {
            // relkind = resultRelInfo->ri_RelationDesc->rd_rel->relkind;
            let relkind = relation_relkind(estate, result_rel_info);
            let row_id_attno = ri_row_id_attno::call(estate, result_rel_info);

            if relkind == RELKIND_RELATION
                || relkind == RELKIND_MATVIEW
                || relkind == RELKIND_PARTITIONED_TABLE
            {
                // ri_RowIdAttNo refers to a ctid attribute.  See the comment in
                // ExecInitModifyTable().
                //   datum = ExecGetJunkAttribute(slot, ri_RowIdAttNo, &isNull);
                let attr = exec_get_junk_attribute::call(estate, plan_slot, row_id_attno)?;
                let is_null = attr.isnull;

                // For commands other than MERGE, any tuples having a null row
                // identifier are errors.  For MERGE, we may need to handle them
                // as WHEN NOT MATCHED clauses if any, so do that.
                if is_null {
                    if operation == CmdType::CMD_MERGE {
                        eval_plan_qual_set_slot(&mut node.mt_epqstate, plan_slot);

                        let toprel = node.resultRelInfo[0];
                        let slot = merge::ExecMerge(
                            mcx,
                            &mut context,
                            node,
                            estate,
                            toprel,
                            None,
                            None,
                            node.canSetTag,
                        )?;

                        if slot.is_some() {
                            return Ok(slot);
                        }

                        continue; // continue with the next tuple
                    }

                    return Err(PgError::error("ctid is NULL"));
                }

                // tupleid = (ItemPointer) DatumGetPointer(datum);
                // tuple_ctid = *tupleid;  (be sure we don't free ctid!!)
                // tupleid = &tuple_ctid;
                tuple_ctid = datum_get_item_pointer::call(&attr.value);
                tupleid = Some(&tuple_ctid);
            } else if attribute_number_is_valid(row_id_attno) {
                // Use the wholerow attribute, when available, to reconstruct the
                // old relation tuple.
                //   datum = ExecGetJunkAttribute(slot, ri_RowIdAttNo, &isNull);
                let attr = exec_get_junk_attribute::call(estate, plan_slot, row_id_attno)?;
                let is_null = attr.isnull;

                if is_null {
                    if operation == CmdType::CMD_MERGE {
                        eval_plan_qual_set_slot(&mut node.mt_epqstate, plan_slot);

                        let toprel = node.resultRelInfo[0];
                        let slot = merge::ExecMerge(
                            mcx,
                            &mut context,
                            node,
                            estate,
                            toprel,
                            None,
                            None,
                            node.canSetTag,
                        )?;

                        if slot.is_some() {
                            return Ok(slot);
                        }

                        continue; // continue with the next tuple
                    }

                    return Err(PgError::error("wholerow is NULL"));
                }

                // oldtupdata.t_data = DatumGetHeapTupleHeader(datum);
                // oldtupdata.t_len = HeapTupleHeaderGetDatumLength(oldtupdata.t_data);
                // ItemPointerSetInvalid(&(oldtupdata.t_self));
                // Historically, view triggers see invalid t_tableOid.
                let tableoid = if relkind == ::types_tuple::access::RELKIND_VIEW {
                    INVALID_OID
                } else {
                    relation_relid(estate, result_rel_info)
                };
                let oldtupdata = datum_get_wholerow_heap_tuple::call(mcx, &attr.value, tableoid)?;
                oldtuple = Some(oldtupdata);
            } else {
                // Only foreign tables are allowed to omit a row-ID attr.
                // Assert(relkind == RELKIND_FOREIGN_TABLE);
            }
        }

        match operation {
            CmdType::CMD_INSERT => {
                // Initialize projection info if first time for this table.
                //   if (unlikely(!resultRelInfo->ri_projectNewInfoValid))
                //       ExecInitInsertProjection(node, resultRelInfo);
                //
                // `ResultRelInfo.ri_projectNewInfoValid` is not yet modeled; the
                // insert-projection bookkeeping lands with that field. Call the
                // initializer unconditionally is wrong, so this guard cannot be
                // expressed and ExecGetInsertNewTuple needs the projection set up.
                if !result_rel_info_project_new_info_valid(estate, result_rel_info) {
                    insert::ExecInitInsertProjection(mcx, node, estate, result_rel_info)?;
                }
                slot = Some(insert::ExecGetInsertNewTuple(
                    estate,
                    result_rel_info,
                    plan_slot,
                )?);
                slot = insert_exec::ExecInsert(
                    mcx,
                    &mut context,
                    node,
                    estate,
                    result_rel_info,
                    slot.unwrap(),
                    node.canSetTag,
                    None,
                    None,
                )?;
            }

            CmdType::CMD_UPDATE => {
                let mut tuplock = false;

                // Initialize projection info if first time for this table.
                if !result_rel_info_project_new_info_valid(estate, result_rel_info) {
                    update::ExecInitUpdateProjection(mcx, node, estate, result_rel_info)?;
                }

                // Make the new tuple by combining plan's output tuple with the
                // old tuple being updated.
                //   oldSlot = resultRelInfo->ri_oldTupleSlot;
                let old_slot = estate
                    .result_rel(result_rel_info)
                    .ri_oldTupleSlot
                    .expect("ExecUpdate: ri_oldTupleSlot not initialized");
                if let Some(ot) = oldtuple.as_ref() {
                    // Assert(!resultRelInfo->ri_needLockTagTuple);
                    // Use the wholerow junk attr as the old tuple.
                    let formed = ot.clone_in(mcx)?;
                    execTuples_seams::exec_force_store_formed_heap_tuple::call(
                        estate, old_slot, formed, false,
                    )?;
                } else {
                    // Fetch the most recent version of old tuple.
                    //   Relation relation = resultRelInfo->ri_RelationDesc;
                    let tid = *tupleid.expect("ExecUpdate: tupleid is NULL");
                    if estate.result_rel(result_rel_info).ri_needLockTagTuple {
                        let relid = relation_relid(estate, result_rel_info);
                        lmgr_seams::lock_tuple::call(
                            relid,
                            tid,
                            INPLACE_UPDATE_TUPLE_LOCK,
                        )?;
                        tuplock = true;
                    }
                    let rel = relation_alias(estate, result_rel_info);
                    let any = snapshot_any();
                    let mcx = estate.es_query_cxt;
                    let oldslot_ref = estate.slot_data_mut(old_slot);
                    if !table_tableam::table_tuple_fetch_row_version(
                        mcx, &rel, &tid, &any, oldslot_ref,
                    )? {
                        return Err(PgError::error("failed to fetch tuple being updated"));
                    }
                }
                slot = Some(update::ExecGetUpdateNewTuple(
                    estate,
                    result_rel_info,
                    plan_slot,
                    Some(old_slot),
                )?);

                // Now apply the update.
                slot = update::ExecUpdate(
                    mcx,
                    &mut context,
                    node,
                    estate,
                    result_rel_info,
                    tupleid,
                    oldtuple,
                    Some(old_slot),
                    slot.unwrap(),
                    node.canSetTag,
                )?;
                if tuplock {
                    let relid = relation_relid(estate, result_rel_info);
                    lmgr_seams::unlock_tuple::call(
                        relid,
                        *tupleid.expect("ExecUpdate: tupleid is NULL"),
                        INPLACE_UPDATE_TUPLE_LOCK,
                    )?;
                }
            }

            CmdType::CMD_DELETE => {
                slot = delete_exec::ExecDelete(
                    mcx,
                    &mut context,
                    node,
                    estate,
                    result_rel_info,
                    tupleid,
                    oldtuple,
                    true,
                    false,
                    node.canSetTag,
                    None,
                    None,
                    None,
                )?;
            }

            CmdType::CMD_MERGE => {
                slot = merge::ExecMerge(
                    mcx,
                    &mut context,
                    node,
                    estate,
                    result_rel_info,
                    tupleid,
                    oldtuple,
                    node.canSetTag,
                )?;
            }

            _ => {
                // elog(ERROR, "unknown operation");
                return Err(PgError::error("unknown operation"));
            }
        }

        // If we got a RETURNING result, return it to caller.  We'll continue the
        // work on next call.
        if slot.is_some() {
            return Ok(slot);
        }
    }

    // Insert remaining tuples for batch insert.
    //   if (estate->es_insert_pending_result_relations != NIL)
    //       ExecPendingInserts(estate);
    if !estate.es_insert_pending_result_relations.is_empty() {
        insert::ExecPendingInserts(mcx, estate)?;
    }

    // We're done, but fire AFTER STATEMENT triggers before exiting.
    lifecycle::fireASTriggers(node, estate)?;

    node.mt_done = true;

    Ok(None)
}

/// `AttributeNumberIsValid(attno)` (access/attnum.h) — a valid attribute number
/// is non-zero (`InvalidAttrNumber == 0`).
#[inline]
fn attribute_number_is_valid(attno: i32) -> bool {
    attno != 0
}

/// `ResetPerTupleExprContext(estate)` (executor/executor.h) — reset
/// `estate->es_per_tuple_exprcontext` if it exists. The per-tuple ExprContext
/// reset is owned by execUtils.c; modeled here as the no-op the reset is on the
/// trimmed `ExprContext` (which carries no per-tuple memory yet) when present.
#[inline]
fn reset_per_tuple_expr_context(estate: &mut EStateData<'_>) {
    // GetPerTupleExprContext(estate) is es_per_tuple_exprcontext; ResetExprContext
    // frees its ecxt_per_tuple_memory. The per-tuple memory context is owned by
    // execUtils.c and not yet carried on the trimmed `ExprContext`, so there is
    // nothing to free here yet; the reset lands with that owner.
    let _ = estate.es_per_tuple_exprcontext;
}

/// `ResetExprContext(econtext)` (executor/executor.h) — free the context's
/// per-tuple memory. The reset is owned by execUtils.c; the trimmed
/// `ExprContext` carries no per-tuple memory yet, so this is a no-op until that
/// owner lands.
#[inline]
fn reset_expr_context(estate: &mut EStateData<'_>, ecxt: ::nodes::EcxtId) {
    let _ = (estate, ecxt);
}

/// `EvalPlanQualSetSlot(epqstate, slot)` (executor/execMain.c) — record the
/// original output slot for a possible later EPQ recheck (`epqstate->origslot
/// = slot;`).
///
/// `origslot` is one of the EvalPlanQual-machinery fields trimmed from the
/// canonical owned [`::nodes::EPQState`] (it lands with the execMain
/// EvalPlanQual port, which both writes and consumes it via `es_epq_active`).
/// In the owned model the modifytable port never reads `origslot` back — the
/// EPQ recheck that consumes it is itself an execMain seam — so the faithful
/// residue of this setter is a no-op until execMain owns the field.
#[inline]
fn eval_plan_qual_set_slot(epqstate: &mut ::nodes::EPQState<'_>, slot: SlotId) {
    // C: epqstate->origslot = slot; — origslot is trimmed from the canonical
    // EPQState (owned by execMain's EvalPlanQual machinery); no-op residue.
    let _ = (epqstate, slot);
}

/// `resultRelInfo->ri_projectNewInfoValid` — whether the insert/update "new
/// tuple" projection (`ri_projectNew`/`ri_newTupleSlot`/`ri_oldTupleSlot`) has
/// been built. `ExecInitInsertProjection`/`ExecInitUpdateProjection` set it true.
#[inline]
fn result_rel_info_project_new_info_valid(estate: &EStateData<'_>, rri: RriId) -> bool {
    estate.result_rel(rri).ri_projectNewInfoValid
}

/// `resultRelInfo->ri_RelationDesc->rd_rel->relkind`.
fn relation_relkind(estate: &EStateData<'_>, rri: RriId) -> u8 {
    estate
        .result_rel(rri)
        .ri_RelationDesc
        .as_ref()
        .expect("ResultRelInfo has no relation")
        .rd_rel
        .relkind
}

/// `RelationGetRelid(resultRelInfo->ri_RelationDesc)`.
fn relation_relid(estate: &EStateData<'_>, rri: RriId) -> types_core::Oid {
    estate
        .result_rel(rri)
        .ri_RelationDesc
        .as_ref()
        .expect("ResultRelInfo has no relation")
        .rd_id
}

/// An `alias()` of `ri_RelationDesc` (shared, no release authority).
pub(crate) fn relation_alias<'mcx>(
    estate: &EStateData<'mcx>,
    rri: RriId,
) -> rel::Relation<'mcx> {
    estate
        .result_rel(rri)
        .ri_RelationDesc
        .as_ref()
        .expect("ResultRelInfo has no relation")
        .alias()
}

/// `SnapshotAny` (snapmgr) — the static "any tuple visible" snapshot.
pub(crate) fn snapshot_any() -> Option<snapshot::SnapshotData> {
    Some(snapshot::SnapshotData::sentinel(snapshot::SnapshotType::SNAPSHOT_ANY))
}

