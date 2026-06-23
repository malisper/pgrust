//! Install bodies for the 24 `backend-executor-nodeLockRows` seams
//! (`nodeLockRows.c`'s calls into subsystems below the executor-node layer).
//!
//! `nodeLockRows` is a leaf executor node whose state machine is ported in its
//! own crate, but it reaches the cross-node `ExecProcNode` dispatch, the
//! `table_tuple_lock` table-AM call, the EvalPlanQual machinery (`execMain.c`),
//! the junk-attribute fetch and result-type/slot-ops setup (`execTuples.c` /
//! `execUtils.c`), the rowmark lookup/build (`ExecFindRowMark` /
//! `ExecBuildAuxRowMark`, `execMain.c`), and the `XactIsoLevel` GUC through
//! per-owner seams. This dispatch crate (`execProcnode.c`) owns the
//! `ExecInitLockRows`/`ExecProcNode` call sites and already depends on the
//! execTuples/execUtils/tableam substrate, so it installs those seam bodies
//! here — the same precedent as `cte_seams.rs`.
//!
//! The EvalPlanQual *recheck* leg (`EvalPlanQualBegin`/`SetSlot`/`Next`) needs
//! the recheck exec sub-tree (`EPQState.recheckplanstate`/`recheckestate`) that
//! `execMain.c`'s `EvalPlanQualStart` builds — substrate not yet modelled — so
//! those three seams are honest loud errors. They are only reached when a lock
//! traversed a concurrent update chain (`tmfd.traversed` / FDW `updated`); the
//! in-snapshot lock path (`TM_Ok` with `!traversed`) never calls them.

#![allow(non_snake_case)]

use mcx::PgBox;
use types_core::primitive::{AttrNumber, Index};
use types_error::{PgError, PgResult};
use nodes::execnodes::EStateData;
use nodes::nodelockrows::{
    ExecAuxRowMarkData, ExecRowMark, LockRows, LockRowsStateData, LockWaitError, LockWaitSkip,
    ROW_MARK_COPY,
};
use nodes::{PlanStateNode, SlotId};

use nodeLockRows_seams as seam;
use execProcnode_seams as procnode;
use execTuples_seams as execTuples;

/// The `PlanState.ExecProcNode` callback installed by `ExecInitLockRows` (via
/// the `init_plan_state_links` seam): `castNode(LockRowsState, pstate)`, run
/// `ExecLockRows`, and translate its `PgResult<bool>` (the working "outer" slot
/// is left in `node.lr_curOuterSlot`) into the `ExecProcNodeMtd` `Option<SlotId>`.
fn exec_lock_rows_node<'mcx>(
    pstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let node = match pstate {
        PlanStateNode::LockRows(node) => node,
        other => panic!("castNode(LockRowsState, pstate) failed: {other:?}"),
    };
    let have = nodeLockRows::ExecLockRows(node, estate)?;
    Ok(if have { node.lr_curOuterSlot } else { None })
}

/// Get-or-create the per-rti EvalPlanQual test slot for the `mark_index`-th
/// rowmark, returning its pool [`SlotId`]. C's `EvalPlanQualSlot(epqstate,
/// relation, rti)` lazily `table_slot_create`s a slot of the relation's tuple
/// descriptor into `epqstate->relsubs_slot[rti-1]` and returns it.
fn epq_slot_for_mark<'mcx>(
    node: &mut LockRowsStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    mark_index: usize,
) -> PgResult<SlotId> {
    let rti = aux_rowmark(node, mark_index)?
        .rowmark
        .as_ref()
        .expect("aux rowmark has no ExecRowMark")
        .rti;
    let idx = (rti - 1) as usize;

    // EvalPlanQualInit pre-allocated relsubs_slot with rtsize None entries.
    let slots = node
        .lr_epqstate
        .relsubs_slot
        .as_ref()
        .expect("lr_epqstate.relsubs_slot must be allocated by EvalPlanQualInit");
    if let Some(existing) = slots[idx] {
        return Ok(existing);
    }

    // First use for this relation: table_slot_create(erm->relation).
    let rel = aux_rowmark(node, mark_index)?
        .rowmark
        .as_ref()
        .expect("aux rowmark has no ExecRowMark")
        .relation
        .as_ref()
        .expect("EvalPlanQualSlot: locking rowmark has no relation")
        .alias();
    let mcx = estate.es_query_cxt;
    let slot = table_tableam::table_slot_create(mcx, &rel)?;
    let id = estate.push_slot_data(slot)?;
    node.lr_epqstate.relsubs_slot.as_mut().unwrap()[idx] = Some(id);
    Ok(id)
}

/// Borrow the `mark_index`-th `ExecAuxRowMark`.
fn aux_rowmark<'a, 'mcx>(
    node: &'a LockRowsStateData<'mcx>,
    mark_index: usize,
) -> PgResult<&'a ExecAuxRowMarkData<'mcx>> {
    node.lr_arowMarks
        .get(mark_index)
        .ok_or_else(|| PgError::error("lockrows: aux rowmark index out of range"))
}

/// Bridge the `EvalPlanQualNext` recheck output (a RECHECK-estate slot `s`) into
/// a PARENT-estate slot, so the LockRows node's working "outer" slot — which the
/// caller (and any node above) addresses in the parent estate — is valid.
///
/// C returns the recheck-estate slot directly (valid by pointer across estates);
/// the owned `SlotId` is estate-local, so we copy the tuple across with a
/// fetch-heap-tuple / force-store bridge (the same shape `EvalPlanQual` uses for
/// the ModifyTable path). The parent result slot is created lazily with the
/// recheck output's tuple descriptor and cached on `lr_epqstate
/// .epq_parent_result_slot`.
fn bridge_recheck_slot_to_parent<'mcx>(
    node: &mut LockRowsStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    s: SlotId,
) -> PgResult<SlotId> {
    // Fetch the recheck output as a heap tuple + capture its descriptor, both
    // cloned into the parent's per-query context.
    let pcx = estate.es_query_cxt;
    let (tuple, desc) = {
        let rc = node
            .lr_epqstate
            .recheckestate
            .as_deref_mut()
            .expect("bridge_recheck_slot_to_parent: recheckestate present");
        let rcx = rc.es_query_cxt;
        let (t, _should_free) =
            execTuples::exec_fetch_slot_heap_tuple::call(rcx, rc.slot_data_mut(s), true)?;
        let d = rc
            .slot(s)
            .tts_tupleDescriptor
            .as_deref()
            .map(|d| d.clone_in(pcx))
            .transpose()?;
        (t.clone_in(pcx)?, d)
    };

    // Lazily create the parent result slot (heap-tuple slot of the recheck
    // output's descriptor).
    let pslot = match node.lr_epqstate.epq_parent_result_slot {
        Some(ps) => ps,
        None => {
            let desc_owned = match desc.as_ref() {
                Some(d) => Some(mcx::alloc_in(pcx, d.clone_in(pcx)?)?),
                None => None,
            };
            let ps = execTuples::exec_alloc_table_slot::call(
                estate,
                desc_owned,
                nodes::TupleSlotKind::HeapTuple,
            )?;
            node.lr_epqstate.epq_parent_result_slot = Some(ps);
            ps
        }
    };
    execTuples::exec_force_store_formed_heap_tuple::call(estate, pslot, tuple, true)?;
    Ok(pslot)
}

/// `ExecFindRowMark(estate, rti, missing_ok)` (execMain.c): find the
/// `ExecRowMark` for the given range-table index in `estate->es_rowmarks`.
/// `missing_ok` is always `false` at the live call site. The owned model holds
/// the rowmark by value in `es_rowmarks[rti-1]`; this hands a fresh
/// (relation-aliased) copy to the caller (the aux mark owns it), matching the
/// C alias-to-the-es_rowmarks-entry the node mutates.
fn ExecFindRowMark<'mcx>(
    estate: &mut EStateData<'mcx>,
    rti: Index,
) -> PgResult<PgBox<'mcx, ExecRowMark<'mcx>>> {
    let idx = (rti - 1) as usize;
    let src = estate
        .es_rowmarks
        .get(idx)
        .and_then(|o| o.as_ref())
        .ok_or_else(|| PgError::error("failed to find ExecRowMark for rangetable index"))?;
    // Copy the entry's scalar fields and re-alias its relation handle (Rc-backed
    // alias of the es_relations-owned open).
    let copy = ExecRowMark {
        relation: src.relation.as_ref().map(|r| r.alias()),
        relid: src.relid,
        rti: src.rti,
        prti: src.prti,
        rowmarkId: src.rowmarkId,
        markType: src.markType,
        strength: src.strength,
        waitPolicy: src.waitPolicy,
        ermActive: src.ermActive,
        curCtid: src.curCtid,
        ermExtra: None,
    };
    let mcx = estate.es_query_cxt;
    mcx::alloc_in(mcx, copy)
}

/// `ExecBuildAuxRowMark(erm, targetlist)` (execMain.c): build the
/// `ExecAuxRowMark` for a rowmark by locating its resjunk ctid/tableoid/wholerow
/// columns in the outer plan's target list. Ported 1:1.
fn ExecBuildAuxRowMark<'mcx>(
    erm: PgBox<'mcx, ExecRowMark<'mcx>>,
    node: &LockRows<'mcx>,
) -> PgResult<ExecAuxRowMarkData<'mcx>> {
    // The outer plan's target list. In C `make_lockrows` sets
    // `plan->targetlist = lefttree->targetlist` (a shared pointer), and
    // `create_plan`'s top-level `apply_tlist_labeling` labels that one list, so
    // C's `ExecBuildAuxRowMark(erm, outerPlan->targetlist)` reads the labeled
    // (resname/resjunk-carrying) list. The owned model clones the tlist into the
    // LockRows node, so the *labeled* copy is the LockRows node's own
    // `plan.targetlist` (the subplan's clone stays unlabeled); read it here.
    let targetlist = node.plan.targetlist.as_ref();

    let mut aerm = ExecAuxRowMarkData {
        rowmark: None,
        ctidAttNo: 0,
        toidAttNo: 0,
        wholeAttNo: 0,
    };

    // Look up the resjunk columns associated with this rowmark.
    if erm.markType != ROW_MARK_COPY {
        // need ctid for all methods other than COPY
        let resname = format!("ctid{}", erm.rowmarkId);
        aerm.ctidAttNo = find_junk_in_tlist(targetlist, &resname);
        if aerm.ctidAttNo == 0 {
            return Err(PgError::error(format!(
                "could not find junk {resname} column"
            )));
        }
        // if child relation, need tableoid too
        if erm.rti != erm.prti {
            let resname = format!("tableoid{}", erm.rowmarkId);
            aerm.toidAttNo = find_junk_in_tlist(targetlist, &resname);
            if aerm.toidAttNo == 0 {
                return Err(PgError::error(format!(
                    "could not find junk {resname} column"
                )));
            }
        }
    } else {
        // need the whole row not a TID
        let resname = format!("wholerow{}", erm.rowmarkId);
        aerm.wholeAttNo = find_junk_in_tlist(targetlist, &resname);
        if aerm.wholeAttNo == 0 {
            return Err(PgError::error(format!(
                "could not find junk {resname} column"
            )));
        }
    }

    aerm.rowmark = Some(erm);
    Ok(aerm)
}

/// `ExecFindJunkAttributeInTlist(targetlist, attrName)` (execJunk.c): the resno
/// of the resjunk `TargetEntry` whose `resname` matches, or 0
/// (`InvalidAttrNumber`) if none.
fn find_junk_in_tlist<'mcx>(
    targetlist: Option<&mcx::PgVec<'mcx, nodes::primnodes::TargetEntry<'mcx>>>,
    attr_name: &str,
) -> AttrNumber {
    let Some(tlist) = targetlist else {
        return 0;
    };
    for tle in tlist.iter() {
        if tle.resjunk {
            if let Some(name) = tle.resname.as_deref() {
                if name == attr_name {
                    return tle.resno;
                }
            }
        }
    }
    0
}

/// Install every `nodeLockRows` seam. Called from this crate's `init_seams()`.
pub fn init_seams() {
    // --- cross-node dispatch (execProcnode.c) -----------------------------

    // slot = ExecProcNode(outerPlanState(node)); fold TupIsNull → Ok(false);
    // stash the produced slot id as the node's working "outer" slot.
    seam::exec_proc_node_outer::set(|node, estate| {
        let outer = node
            .ps
            .lefttree
            .as_deref_mut()
            .ok_or_else(|| PgError::error("ExecLockRows: outer plan state is NULL"))?;
        let slot = procnode::exec_proc_node::call(outer, estate)?;
        let slot = match slot {
            Some(id) if !estate.slot(id).is_empty() => Some(id),
            _ => None,
        };
        node.lr_curOuterSlot = slot;
        Ok(slot.is_some())
    });

    // outerPlanState(lrstate) = ExecInitNode(outerPlan(node), estate, eflags).
    seam::exec_init_node_outer::set(|lrstate, node, estate, eflags| {
        let mcx = estate.es_query_cxt;
        let outer_plan = node.plan.lefttree.as_deref();
        lrstate.ps.lefttree = procnode::exec_init_node::call(mcx, outer_plan, estate, eflags)?;
        Ok(())
    });

    // ExecEndNode(outerPlanState(node)).
    seam::exec_end_node_outer::set(|node, estate| match node.ps.lefttree.as_deref_mut() {
        Some(outer) => procnode::exec_end_node::call(outer, estate),
        None => Ok(()),
    });

    // outerPlan->chgParam == NULL.
    seam::outer_chg_param_is_null::set(|node| {
        Ok(node
            .ps
            .lefttree
            .as_deref()
            .map(|o| o.ps_head().chgParam.is_none())
            .unwrap_or(true))
    });

    // ExecReScan(outerPlanState(node)).
    seam::exec_rescan_outer::set(|node, estate| match node.ps.lefttree.as_deref_mut() {
        Some(outer) => execAmi_seams::exec_re_scan::call(outer, estate),
        None => Ok(()),
    });

    // lrstate->ps.plan = (Plan *) node; lrstate->ps.ExecProcNode = ExecLockRows.
    seam::init_plan_state_links::set(|lrstate, _node| {
        lrstate.ps.ExecProcNode = Some(exec_lock_rows_node);
        Ok(())
    });

    // CHECK_FOR_INTERRUPTS().
    seam::check_for_interrupts::set(|| postgres_seams::check_for_interrupts::call());

    // --- result-type / slot-ops setup (execTuples.c / execUtils.c) --------

    // ExecInitResultTypeTL(&lrstate->ps).
    seam::exec_init_result_type_tl::set(|lrstate, estate| {
        execTuples::exec_init_result_type_tl::call(&mut lrstate.ps, estate)
    });

    // lrstate->ps.resultopsset = true;
    // lrstate->ps.resultops = ExecGetResultSlotOps(outerPlanState(lrstate),
    //                                              &lrstate->ps.resultopsfixed);
    seam::exec_get_result_slot_ops::set(|lrstate| {
        // The outer plan state is needed; thread the EState via the node's own
        // ps.lefttree (this seam carries no estate, but ExecGetResultSlotOps
        // reads only the planstate head's resultops/ps_ResultTupleSlot, which
        // the node copies from the already-initialized child). The child's
        // result ops were set during its ExecInitNode; copy them across.
        lrstate.ps.resultopsset = true;
        let (ops, fixed) = match lrstate.ps.lefttree.as_deref() {
            Some(outer) => (outer.ps_head().resultops, outer.ps_head().resultopsfixed),
            None => (None, false),
        };
        lrstate.ps.resultops = ops;
        lrstate.ps.resultopsfixed = fixed;
        Ok(())
    });

    // --- per-rowmark slot / junk fetch (execMain.c / execTuples.c) ---------

    // markSlot = EvalPlanQualSlot(...); ExecClearTuple(markSlot).
    seam::eval_plan_qual_slot_clear::set(|node, estate, mark_index| {
        let slot = epq_slot_for_mark(node, estate, mark_index)?;
        execTuples::exec_clear_tuple::call(estate, slot)
    });

    // DatumGetObjectId(ExecGetJunkAttribute(slot, aerm->toidAttNo, &isNull)).
    seam::exec_get_junk_tableoid::set(|node, estate, mark_index, is_null| {
        let (slot, attno) = junk_src(node, mark_index, JunkKind::TableOid)?;
        let attr = execTuples::slot_getattr_by_id::call(estate, slot, attno)?;
        *is_null = attr.isnull;
        Ok(attr.value.as_oid())
    });

    // tid = *((ItemPointer) DatumGetPointer(ExecGetJunkAttribute(slot,
    //         aerm->ctidAttNo, &isNull))).
    seam::exec_get_junk_ctid::set(|node, estate, mark_index, is_null| {
        let (slot, attno) = junk_src(node, mark_index, JunkKind::Ctid)?;
        let attr = execTuples::slot_getattr_by_id::call(estate, slot, attno)?;
        *is_null = attr.isnull;
        if attr.isnull {
            return Ok(types_tuple::heaptuple::ItemPointerData::default());
        }
        Ok(heaptuple::item_pointer_from_bytes(
            attr.value.as_ref_bytes(),
        ))
    });

    // erm->relation->rd_rel->relkind.
    seam::relation_get_relkind::set(|node, mark_index| {
        let erm = aux_rowmark(node, mark_index)?
            .rowmark
            .as_ref()
            .ok_or_else(|| PgError::error("aux rowmark has no ExecRowMark"))?;
        let rel = erm
            .relation
            .as_ref()
            .ok_or_else(|| PgError::error("rowmark relation is NULL"))?;
        Ok(rel.rd_rel.relkind)
    });

    // --- table-AM lock (tableam.c) ----------------------------------------

    // test = table_tuple_lock(erm->relation, &tid, estate->es_snapshot,
    //     markSlot, estate->es_output_cid, request.lockmode, erm->waitPolicy,
    //     request.lockflags, &tmfd).
    seam::table_tuple_lock::set(|node, estate, mark_index, tid, request, tmfd| {
        let mark_slot = epq_slot_for_mark(node, estate, mark_index)?;
        let erm = aux_rowmark(node, mark_index)?
            .rowmark
            .as_ref()
            .ok_or_else(|| PgError::error("aux rowmark has no ExecRowMark"))?;
        let rel = erm
            .relation
            .as_ref()
            .ok_or_else(|| PgError::error("rowmark relation is NULL"))?
            .alias();
        let wait = wait_policy_to_tableam(erm.waitPolicy);
        let snapshot = estate.es_snapshot.as_deref().cloned();
        let cid = estate.es_output_cid;
        let mcx = estate.es_query_cxt;
        let inslot = estate.slot_data_mut(mark_slot);
        table_tableam::table_tuple_lock(
            mcx,
            &rel,
            &tid,
            &snapshot,
            inslot,
            cid,
            request.lockmode,
            wait,
            request.lockflags as u8,
            tmfd,
        )
    });

    // requests for foreign tables must be passed to their FDW — no FDW is
    // modelled here, so the foreign-table lock path errors (it is only reached
    // when erm->relation->rd_rel->relkind == RELKIND_FOREIGN_TABLE, which
    // CheckValidRowMarkRel rejects unless the FDW supports it).
    seam::refetch_foreign_row::set(|_node, _estate, _mark_index, _tid| {
        Err(PgError::error(
            "RefetchForeignRow: foreign-table row locking (FDW RefetchForeignRow) not ported",
        ))
    });

    // XactIsoLevel.
    seam::xact_iso_level::set(|| Ok(transam_xact::XactIsoLevel()));

    // --- EvalPlanQual machinery (execMain.c) ------------------------------

    // EvalPlanQualInit(&lrstate->lr_epqstate, estate, outerPlan, epq_arowmarks,
    //                  node->epqParam, NIL).
    seam::eval_plan_qual_init::set(|lrstate, node, estate, epq_arowmarks| {
        // EvalPlanQualInit records epqParam + resultRelations (NIL for LockRows)
        // and pre-allocates relsubs_slot; EvalPlanQualSetPlan then records the
        // recheck plan (= outerPlan) and the non-locking aux rowmarks.
        let mcx = estate.es_query_cxt;
        let rtsize = estate.es_range_table_size;
        lrstate.lr_epqstate.epqParam = node.epqParam;
        let mut slots = mcx::vec_with_capacity_in(mcx, rtsize)?;
        slots.resize(rtsize, None);
        lrstate.lr_epqstate.relsubs_slot = Some(slots);
        lrstate.lr_epqstate.resultRelations = None;
        lrstate.lr_epqstate.relsubs_rowmark = None;
        lrstate.lr_epqstate.relsubs_done = None;
        lrstate.lr_epqstate.relsubs_blocked = None;

        // EvalPlanQualSetPlan(&lrstate->lr_epqstate, outerPlan, epq_arowmarks):
        //   record the recheck plan tree (the LockRows outer plan) + the
        //   non-locking aux rowmarks the init pass partitioned off.
        lrstate.lr_epqstate.plan = node.plan.lefttree.as_deref();
        lrstate.lr_epqstate.arowMarks = if epq_arowmarks.is_empty() {
            None
        } else {
            Some(epq_arowmarks)
        };
        Ok(())
    });

    // EvalPlanQualEnd(&node->lr_epqstate) — release EPQ resources (end the
    // recheck plan + subplans, reset the recheck estate's tuple table, close the
    // result/trigger + range-table relations it opened, free the recheck
    // estate). Idempotent: a no-op when EPQ was never started.
    seam::eval_plan_qual_end::set(|node, estate| {
        execMain_seams::eval_plan_qual_end::call(estate, &mut node.lr_epqstate)
    });

    // EvalPlanQualBegin(&node->lr_epqstate): build (or reset) the recheck estate
    // + plan, then bridge the parent's locked source tuples into the recheck
    // marker so the recheck scans return them.
    seam::eval_plan_qual_begin::set(|node, estate| {
        execMain_seams::eval_plan_qual_begin_lockrows::call(
            estate,
            &mut node.lr_epqstate,
        )
    });

    // EvalPlanQualSetSlot(&node->lr_epqstate, slot): record origslot and bridge
    // the origin output tuple so EvalPlanQualFetchRowMark can read its junk
    // attributes. `slot` is the node's current working "outer" slot.
    seam::eval_plan_qual_set_slot::set(|node, estate| {
        let slot = node
            .lr_curOuterSlot
            .ok_or_else(|| PgError::error("EvalPlanQualSetSlot: no current outer slot"))?;
        execMain_seams::eval_plan_qual_set_slot_lockrows::call(
            estate,
            &mut node.lr_epqstate,
            slot,
        )
    });

    // slot = EvalPlanQualNext(&node->lr_epqstate): run the recheck plan. The
    // result is a RECHECK-estate slot; copy it into a parent-estate slot so the
    // node's working outer slot (a parent slot) is addressable by the caller, as
    // EvalPlanQual does for the ModifyTable path. Returns Ok(false) on TupIsNull.
    seam::eval_plan_qual_next::set(|node, estate| {
        let rcslot =
            execMain_seams::eval_plan_qual_next::call(&mut node.lr_epqstate)?;
        match rcslot {
            None => {
                node.lr_curOuterSlot = None;
                Ok(false)
            }
            Some(s) => {
                // Empty recheck slot == TupIsNull.
                let empty = node
                    .lr_epqstate
                    .recheckestate
                    .as_deref()
                    .expect("EvalPlanQualNext: recheckestate present")
                    .slot(s)
                    .is_empty();
                if empty {
                    node.lr_curOuterSlot = None;
                    return Ok(false);
                }
                let pslot = bridge_recheck_slot_to_parent(node, estate, s)?;
                node.lr_curOuterSlot = Some(pslot);
                Ok(true)
            }
        }
    });

    // --- range-table helpers (execUtils.c) --------------------------------

    // exec_rt_fetch(rti, estate)->rtekind.
    seam::exec_rt_fetch_rtekind::set(|estate, rti| {
        let rte = execUtils::exec_rt_fetch(rti, estate);
        Ok(rte.rtekind)
    });

    // bms_is_member(rti, estate->es_unpruned_relids).
    seam::unpruned_relids_is_member::set(|estate, rti| {
        Ok(execUtils_bms_is_member(estate, rti))
    });

    // ExecFindRowMark(estate, rc->rti, false).
    seam::exec_find_row_mark::set(|estate, rti| ExecFindRowMark(estate, rti));

    // ExecBuildAuxRowMark(erm, outerPlan->targetlist).
    seam::exec_build_aux_row_mark::set(|_estate, node, erm| ExecBuildAuxRowMark(erm, node));
}

/// `bms_is_member(rti, estate->es_unpruned_relids)`.
fn execUtils_bms_is_member<'mcx>(estate: &EStateData<'mcx>, rti: Index) -> bool {
    nodes_core_seams::bms_is_member::call(
        rti as i32,
        estate.es_unpruned_relids.as_deref(),
    )
}

/// Which junk column to source.
enum JunkKind {
    Ctid,
    TableOid,
}

/// `(slot, attno)` for the `mark_index`-th rowmark's ctid/tableoid junk column:
/// the working "outer" slot (`node.lr_curOuterSlot`) and the resjunk resno.
fn junk_src<'mcx>(
    node: &LockRowsStateData<'mcx>,
    mark_index: usize,
    kind: JunkKind,
) -> PgResult<(SlotId, AttrNumber)> {
    let slot = node
        .lr_curOuterSlot
        .ok_or_else(|| PgError::error("ExecLockRows: no current outer slot"))?;
    let aerm = aux_rowmark(node, mark_index)?;
    let attno = match kind {
        JunkKind::Ctid => aerm.ctidAttNo,
        JunkKind::TableOid => aerm.toidAttNo,
    };
    Ok((slot, attno))
}

/// Map the `nodelockrows::LockWaitPolicy` (an `i32` mirror of
/// `nodes/lockoptions.h`) onto the `types_tableam` `LockWaitPolicy` enum.
fn wait_policy_to_tableam(wait: i32) -> types_tableam::tableam::LockWaitPolicy {
    use types_tableam::tableam::LockWaitPolicy as P;
    if wait == LockWaitSkip {
        P::LockWaitSkip
    } else if wait == LockWaitError {
        P::LockWaitError
    } else {
        // LockWaitBlock (== 0, the default) and any unexpected value.
        P::LockWaitBlock
    }
}
