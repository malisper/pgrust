//! Install bodies for the `backend-executor-nodeTidrangescan` seams
//! (`nodeTidrangescan.c`'s calls into subsystems below the executor-node layer).
//!
//! `nodeTidrangescan` is a leaf scan node whose state machine and the
//! `execScan.c` driver are ported in its own crate, but it reaches the
//! cross-node `ExecProcNode` dispatch, expression compile/eval (`execExpr.c`),
//! the execUtils/execTuples init helpers, the TID-range table-AM calls
//! (`table_beginscan_tidrange` / `table_scan_getnextslot_tidrange` /
//! `table_rescan_tidrange`), and the `execScan.c` EvalPlanQual machinery through
//! per-owner seams. This dispatch crate (`execProcnode.c`) owns the
//! `ExecInitTidRangeScan`/`ExecProcNode` call sites and already depends on the
//! execTuples/execUtils/execExpr/tableam substrate, so it installs those seam
//! bodies here — the same precedent as `lockrows_seams.rs`.
//!
//! `exec_assign_scan_projection_info` and `exec_scan_rescan` are installed by
//! `execScan` (which owns the generic `ExecAssignScanProjectionInfo`/
//! `ExecScanReScan` drivers); everything else is installed here.

#![allow(non_snake_case)]

use ::types_error::{PgError, PgResult};
use ::nodes::execnodes::{EPQState, EStateData};
use ::nodes::primnodes::Expr;
use ::nodes::{PlanStateNode, SlotId};

use nodeTidrangescan_seams as seam;
use execExpr_seams as execExpr;
use execTuples_seams as execTuples;
use execUtils_seams as execUtils;

/// The `PlanState.ExecProcNode` callback installed by `ExecInitTidRangeScan`
/// (via the `init_plan_state_links` seam): `castNode(TidRangeScanState,
/// pstate)`, run `ExecTidRangeScan`, and translate its `PgResult<bool>` into the
/// `ExecProcNodeMtd` `Option<SlotId>` (the produced tuple is the projection
/// result slot when projecting, else the scan tuple slot).
fn exec_tidrange_scan_node<'mcx>(
    pstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let node = match pstate {
        PlanStateNode::TidRangeScan(node) => node,
        other => panic!("castNode(TidRangeScanState, pstate) failed: {other:?}"),
    };
    let have = nodeTidrangescan::ExecTidRangeScan(node, estate)?;
    if !have {
        return Ok(None);
    }
    Ok(node
        .ss
        .ps
        .ps_ProjInfo
        .as_ref()
        .and(node.ss.ps.ps_ResultTupleSlot)
        .or(node.ss.ss_ScanTupleSlot))
}

/// Extract the `qual_index`-th qual's operand `Expr` on the requested side.
fn operand_expr<'a, 'mcx>(
    node: &'a ::nodes::nodetidrangescan::TidRangeScan<'mcx>,
    qual_index: usize,
    side: tidrange::OperandSide,
) -> Option<&'a Expr<'mcx>> {
    let quals = node.tidrangequals.as_ref()?;
    let Expr::OpExpr(op) = quals.get(qual_index)? else {
        return None;
    };
    match side {
        tidrange::OperandSide::Left => op.args.first(),
        tidrange::OperandSide::Right => op.args.get(1),
    }
}

/// Borrow the live `EPQState` (`estate->es_epq_active`); the EPQ-recheck seams
/// are only reached inside an EvalPlanQual recheck, where it is non-NULL.
fn epq<'a, 'mcx>(estate: &'a EStateData<'mcx>) -> PgResult<&'a EPQState<'mcx>> {
    estate
        .es_epq_active
        .as_deref()
        .ok_or_else(|| PgError::error("TidRangeScan EPQ seam reached with es_epq_active = NULL"))
}

/// Borrow the live `EPQState` mutably.
fn epq_mut<'a, 'mcx>(estate: &'a mut EStateData<'mcx>) -> PgResult<&'a mut EPQState<'mcx>> {
    estate
        .es_epq_active
        .as_deref_mut()
        .ok_or_else(|| PgError::error("TidRangeScan EPQ seam reached with es_epq_active = NULL"))
}

pub fn init_seams() {
    // --- node factory / makeNode / plan-state links ------------------------

    // tidrangestate->ss.ps.ExecProcNode = ExecTidRangeScan; (the plan and EState
    // back-links are already set by ExecInitTidRangeScan in the node crate.)
    seam::init_plan_state_links::set(|tidrangestate, _node| {
        tidrangestate.ss.ps.ExecProcNode = Some(exec_tidrange_scan_node);
        Ok(())
    });

    // --- expression evaluation (execExpr.c) --------------------------------

    // exprstate = ExecInitExpr((Expr *) get_leftop/get_rightop(expr),
    //                          &tidstate->ss.ps);
    seam::exec_init_expr::set(|tidstate, node, qual_index, side, estate| {
        let expr = operand_expr(node, qual_index, side)
            .expect("TidRangeScan: qual operand expr missing");
        execExpr::exec_init_expr::call(expr, &mut tidstate.ss.ps, estate)
    });

    // (ItemPointer) DatumGetPointer(ExecEvalExprSwitchContext(exprstate,
    //                                                         econtext, &isNull))
    seam::exec_eval_expr_switch_context::set(|exprstate, econtext, is_null, estate| {
        let (datum, null) =
            execExpr::exec_eval_expr_switch_context::call(exprstate, econtext, estate)?;
        *is_null = null;
        if null {
            return Ok(types_tuple::heaptuple::ItemPointerData::default());
        }
        // DatumGetItemPointer: the `tid` Datum is the canonical by-ref 6-byte
        // ItemPointerData image; decode it back.
        Ok(heaptuple::item_pointer_from_bytes(
            datum.as_ref_bytes(),
        ))
    });

    // tidrangestate->ss.ps.qual = ExecInitQual(node->scan.plan.qual,
    //                                          tidrangestate);
    seam::exec_init_qual::set(|tidrangestate, node, estate| {
        let qual = node.scan.plan.qual.as_deref();
        tidrangestate.ss.ps.qual =
            execExpr::exec_init_qual::call(qual, &mut tidrangestate.ss.ps, estate)?;
        Ok(())
    });

    // --- execUtils / execTuples init helpers -------------------------------

    // ExecAssignExprContext(estate, &tidrangestate->ss.ps);
    seam::exec_assign_expr_context::set(|tidrangestate, estate| {
        execUtils::exec_assign_expr_context::call(estate, &mut tidrangestate.ss.ps)
    });

    // tidrangestate->ss.ss_currentRelation =
    //     ExecOpenScanRelation(estate, node->scan.scanrelid, eflags);
    seam::exec_open_scan_relation::set(|tidrangestate, node, eflags, estate| {
        let rel = execUtils::exec_open_scan_relation::call(estate, node.scan.scanrelid, eflags)?;
        tidrangestate.ss_currentRelation = Some(rel);
        Ok(())
    });

    // ExecInitScanTupleSlot(estate, &tidrangestate->ss, RelationGetDescr(rel),
    //                       table_slot_callbacks(rel));
    seam::exec_init_scan_tuple_slot::set(|tidrangestate, estate| {
        let mcx = estate.es_query_cxt;
        let rel = tidrangestate
            .ss_currentRelation
            .as_ref()
            .expect("ExecInitTidRangeScan: relation not opened");
        let tts_ops = table_tableam::table_slot_callbacks(rel);
        let tupdesc = Some(mcx::alloc_in(mcx, rel.rd_att.clone_in(mcx)?)?);
        execTuples::exec_init_scan_tuple_slot::call(estate, &mut tidrangestate.ss, tupdesc, tts_ops)
    });

    // ExecInitResultTypeTL(&tidrangestate->ss.ps);
    seam::exec_init_result_type_tl::set(|tidrangestate, estate| {
        execTuples::exec_init_result_type_tl::call(&mut tidrangestate.ss.ps, estate)
    });

    // --- per-tuple ExprContext / scan-slot plumbing ------------------------

    // CHECK_FOR_INTERRUPTS().
    seam::check_for_interrupts::set(|| postgres_seams::check_for_interrupts::call());

    // ResetExprContext(node->ss.ps.ps_ExprContext).
    seam::reset_per_tuple_expr_context::set(|node, estate| {
        let econtext = node
            .ss
            .ps
            .ps_ExprContext
            .expect("TidRangeScan: ps_ExprContext not initialized");
        execUtils::reset_expr_context::call(estate, econtext)
    });

    // econtext->ecxt_scantuple = node->ss.ss_ScanTupleSlot.
    seam::set_econtext_scantuple_to_scan_slot::set(|node, estate| {
        let econtext = node
            .ss
            .ps
            .ps_ExprContext
            .expect("TidRangeScan: ps_ExprContext not initialized");
        let slot = node.ss.ss_ScanTupleSlot;
        estate.ecxt_mut(econtext).ecxt_scantuple = slot;
        Ok(())
    });

    // ExecClearTuple(node->ss.ss_ScanTupleSlot).
    seam::exec_clear_scan_tuple::set(|node, estate| {
        if let Some(slot) = node.ss.ss_ScanTupleSlot {
            execTuples::exec_clear_tuple::call(estate, slot)?;
        }
        Ok(())
    });

    // ExecClearTuple(projInfo->pi_state.resultslot).
    seam::exec_clear_proj_result_slot::set(|node, estate| {
        if let Some(slot) = node.ss.ps.ps_ResultTupleSlot {
            execTuples::exec_clear_tuple::call(estate, slot)?;
        }
        Ok(())
    });

    // ExecQual(node->ss.ps.qual, node->ss.ps.ps_ExprContext).
    seam::exec_qual::set(|node, estate| {
        let econtext = node
            .ss
            .ps
            .ps_ExprContext
            .expect("TidRangeScan: ps_ExprContext not initialized");
        match node.ss.ps.qual.as_deref_mut() {
            Some(qual) => execExpr::exec_qual::call(qual, econtext, estate),
            None => Ok(true),
        }
    });

    // ExecProject(node->ss.ps.ps_ProjInfo); always produces a tuple.
    seam::exec_project::set(|node, estate| {
        execExpr::exec_project::call(&mut node.ss.ps, estate)?;
        Ok(true)
    });

    // --- table access methods (TID range) ----------------------------------

    // node->ss.ss_currentScanDesc = table_beginscan_tidrange(
    //     node->ss.ss_currentRelation, estate->es_snapshot,
    //     &node->trss_mintid, &node->trss_maxtid);
    seam::table_beginscan_tidrange::set(|node, estate| {
        let mcx = estate.es_query_cxt;
        let snapshot = estate.es_snapshot.clone();
        let mintid = node.trss_mintid;
        let maxtid = node.trss_maxtid;
        let rel = node
            .ss_currentRelation
            .as_ref()
            .expect("TidRangeNext: ss_currentRelation not opened");
        let scandesc = table_tableam::table_beginscan_tidrange(
            mcx, rel, snapshot, &mintid, &maxtid,
        )?;
        node.ss_currentScanDesc = Some(scandesc);
        Ok(())
    });

    // table_rescan_tidrange(scandesc, &node->trss_mintid, &node->trss_maxtid).
    seam::table_rescan_tidrange::set(|node, estate| {
        let mcx = estate.es_query_cxt;
        let mintid = node.trss_mintid;
        let maxtid = node.trss_maxtid;
        let scandesc = node
            .ss_currentScanDesc
            .as_deref_mut()
            .expect("table_rescan_tidrange: ss_currentScanDesc not set");
        table_tableam::table_rescan_tidrange(mcx, scandesc, &mintid, &maxtid)
    });

    // table_scan_getnextslot_tidrange(scandesc, estate->es_direction,
    //                                 node->ss.ss_ScanTupleSlot).
    seam::table_scan_getnextslot_tidrange::set(|node, estate| {
        let mcx = estate.es_query_cxt;
        let direction = estate.es_direction;
        let slot_id = node
            .ss
            .ss_ScanTupleSlot
            .expect("TidRangeNext: ss_ScanTupleSlot not initialized");
        let scandesc = node
            .ss_currentScanDesc
            .as_deref_mut()
            .expect("TidRangeNext: ss_currentScanDesc not set");
        table_tableam::table_scan_getnextslot_tidrange(
            mcx,
            scandesc,
            direction,
            estate.slot_data_mut(slot_id),
        )
    });

    // table_endscan(node->ss.ss_currentScanDesc).
    seam::table_endscan::set(|node, _estate| {
        if let Some(scandesc) = node.ss_currentScanDesc.take() {
            table_tableam::table_endscan(scandesc)?;
        }
        Ok(())
    });

    // --- EvalPlanQual machinery (execScan.c) -------------------------------

    // ((Scan *) node->ps.plan)->scanrelid.
    seam::scan_scanrelid::set(|node| {
        let plan = node
            .ss
            .ps
            .plan
            .expect("scan_scanrelid: TidRangeScanState has no plan");
        Ok(plan
            .as_tidrangescan()
            .expect("scan_scanrelid: plan is not a TidRangeScan")
            .scan
            .scanrelid)
    });

    // node->ps.state->es_epq_active != NULL.
    seam::es_epq_active_present::set(|_node, estate| Ok(estate.es_epq_active.is_some()));

    // bms_is_member(epqstate->epqParam, node->ps.plan->extParam).
    seam::epq_param_is_member_of_ext_param::set(|node, estate| {
        let epq_param = epq(estate)?.epqParam;
        let plan = node
            .ss
            .ps
            .plan
            .expect("epq_param_is_member_of_ext_param: TidRangeScanState has no plan");
        let ext_param = plan
            .as_tidrangescan()
            .expect("epq_param_is_member_of_ext_param: plan is not a TidRangeScan")
            .scan
            .plan
            .extParam
            .as_deref();
        Ok(nodes_core_seams::bms_is_member::call(
            epq_param, ext_param,
        ))
    });

    // epqstate->relsubs_done[index].
    seam::epq_relsubs_done::set(|_node, index, estate| {
        Ok(epq(estate)?
            .relsubs_done
            .as_ref()
            .map(|v| v[index as usize])
            .unwrap_or(false))
    });

    // epqstate->relsubs_done[index] = value.
    seam::epq_set_relsubs_done::set(|_node, index, value, estate| {
        if let Some(v) = epq_mut(estate)?.relsubs_done.as_mut() {
            v[index as usize] = value;
        }
        Ok(())
    });

    // epqstate->relsubs_slot[index] != NULL.
    seam::epq_relsubs_slot_present::set(|_node, index, estate| {
        Ok(epq(estate)?
            .relsubs_slot
            .as_ref()
            .map(|v| v[index as usize].is_some())
            .unwrap_or(false))
    });

    // Copy epqstate->relsubs_slot[index] into the node's scan slot.
    seam::epq_load_relsubs_slot::set(|node, index, estate| {
        let src = epq(estate)?
            .relsubs_slot
            .as_ref()
            .and_then(|v| v[index as usize])
            .ok_or_else(|| PgError::error("epq_load_relsubs_slot: EPQ slot is NULL"))?;
        let dst = node
            .ss
            .ss_ScanTupleSlot
            .ok_or_else(|| PgError::error("epq_load_relsubs_slot: ss_ScanTupleSlot is NULL"))?;
        execTuples::exec_copy_slot::call(estate, dst, src)
    });

    // epqstate->relsubs_rowmark[index] != NULL.
    seam::epq_relsubs_rowmark_present::set(|_node, index, estate| {
        Ok(epq(estate)?
            .relsubs_rowmark
            .as_ref()
            .map(|v| v[index as usize])
            .unwrap_or(false))
    });

    // EvalPlanQualFetchRowMark(epqstate, scanrelid, node->ss.ss_ScanTupleSlot).
    seam::eval_plan_qual_fetch_row_mark::set(|node, scanrelid, estate| {
        let slot = node
            .ss
            .ss_ScanTupleSlot
            .ok_or_else(|| PgError::error("eval_plan_qual_fetch_row_mark: ss_ScanTupleSlot NULL"))?;
        execMain_seams::eval_plan_qual_fetch_row_mark::call(estate, scanrelid, slot)
    });
}
