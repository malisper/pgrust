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
use types_nodes::execnodes::EStateData;
use types_nodes::nodelockrows::{
    ExecAuxRowMarkData, ExecRowMark, LockRows, LockRowsStateData, LockWaitError, LockWaitSkip,
    ROW_MARK_COPY,
};
use types_nodes::{PlanStateNode, SlotId};

use backend_executor_nodeLockRows_seams as seam;
use backend_executor_execProcnode_seams as procnode;
use backend_executor_execTuples_seams as execTuples;

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
    let have = backend_executor_nodeLockRows::ExecLockRows(node, estate)?;
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
    let slot = backend_access_table_tableam::table_slot_create(mcx, &rel)?;
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
    targetlist: Option<&mcx::PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>>>,
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
        Some(outer) => backend_executor_execAmi_seams::exec_re_scan::call(outer, estate),
        None => Ok(()),
    });

    // lrstate->ps.plan = (Plan *) node; lrstate->ps.ExecProcNode = ExecLockRows.
    seam::init_plan_state_links::set(|lrstate, _node| {
        lrstate.ps.ExecProcNode = Some(exec_lock_rows_node);
        Ok(())
    });

    // CHECK_FOR_INTERRUPTS().
    seam::check_for_interrupts::set(|| backend_tcop_postgres_seams::check_for_interrupts::call());

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
        Ok(backend_access_common_heaptuple::item_pointer_from_bytes(
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
        backend_access_table_tableam::table_tuple_lock(
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
    seam::xact_iso_level::set(|| Ok(backend_access_transam_xact::XactIsoLevel()));

    // --- EvalPlanQual machinery (execMain.c) ------------------------------

    // EvalPlanQualInit(&lrstate->lr_epqstate, estate, outerPlan, epq_arowmarks,
    //                  node->epqParam, NIL).
    seam::eval_plan_qual_init::set(|lrstate, node, estate, _epq_arowmarks| {
        // Record epqParam and pre-allocate the per-rti relsubs_slot array; the
        // recheck sub-tree (relsubs_rowmark/done/blocked, recheckplanstate) is
        // built lazily in EvalPlanQualBegin (not reached on the in-snapshot
        // lock path). The non-locking epq aux rowmarks would feed the recheck
        // plan; the trimmed EPQState rebuilds them from the plan in Begin, so
        // they are dropped here (faithful to the trimmed model).
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
        Ok(())
    });

    // EvalPlanQualEnd(&node->lr_epqstate) — release EPQ resources. On the
    // trimmed EPQState (no recheckestate/tuple_table) this is the idempotent
    // no-op the C end-of-scan / ExecEndLockRows both call.
    seam::eval_plan_qual_end::set(|_node, _estate| Ok(()));

    // The three recheck-only seams. Reached only when a lock traversed a
    // concurrent update chain (tmfd.traversed / FDW updated); the in-snapshot
    // lock path never calls them. They need EvalPlanQualStart's recheck exec
    // sub-tree (EPQState.recheckplanstate / recheckestate), which is not yet
    // modelled, so they are honest loud errors.
    seam::eval_plan_qual_begin::set(|_node, _estate| {
        Err(PgError::error(
            "EvalPlanQualBegin: EPQ recheck on concurrent update needs the recheck exec \
             sub-tree (EPQState.recheckplanstate / recheckestate via EvalPlanQualStart), \
             not yet modelled",
        ))
    });
    seam::eval_plan_qual_set_slot::set(|_node, _estate| {
        Err(PgError::error(
            "EvalPlanQualSetSlot: EPQ recheck on concurrent update needs the recheck exec \
             sub-tree (EPQState.origslot / recheckplanstate), not yet modelled",
        ))
    });
    seam::eval_plan_qual_next::set(|_node, _estate| {
        Err(PgError::error(
            "EvalPlanQualNext: EPQ recheck on concurrent update needs the recheck exec \
             sub-tree (EvalPlanQualStart builds recheckplanstate/recheckestate), not yet \
             modelled",
        ))
    });

    // --- range-table helpers (execUtils.c) --------------------------------

    // exec_rt_fetch(rti, estate)->rtekind.
    seam::exec_rt_fetch_rtekind::set(|estate, rti| {
        let rte = backend_executor_execUtils::exec_rt_fetch(rti, estate);
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
    backend_nodes_core_seams::bms_is_member::call(
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
