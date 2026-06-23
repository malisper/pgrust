//! Install bodies for the `backend-executor-nodeSamplescan` seams
//! (`nodeSamplescan.c`'s calls into subsystems below the executor-node layer).
//!
//! `nodeSamplescan` is a leaf scan node whose state machine and the `execScan.c`
//! driver are ported in its own crate, but it reaches the cross-node
//! `ExecProcNode` dispatch, expression compile/eval (`execExpr.c`), the
//! execUtils/execTuples init helpers, the TABLESAMPLE table-AM calls
//! (`table_beginscan_sampling` / `table_rescan_set_params` /
//! `table_scan_sample_next_block` / `table_scan_sample_next_tuple`), the
//! PRNG/hash helpers, and the `execScan.c` EvalPlanQual machinery through
//! per-owner seams. This dispatch crate (`execProcnode.c`) owns the
//! `ExecInitSampleScan`/`ExecProcNode` call sites and already depends on the
//! execTuples/execUtils/execExpr/tableam substrate, so it installs those seam
//! bodies here — the same precedent as `tidrangescan_seams.rs`.
//!
//! `exec_assign_scan_projection_info` and `exec_scan_rescan` are installed by
//! `execScan` (which owns the generic `ExecAssignScanProjectionInfo`/
//! `ExecScanReScan` drivers); everything else is installed here.

#![allow(non_snake_case)]

use ::types_error::{PgError, PgResult};
use ::nodes::execnodes::{EPQState, EStateData};
use ::nodes::{PlanStateNode, SlotId};
use ::samplescan::SampleScanState;

use nodeSamplescan_seams as seam;
use execExpr_seams as execExpr;
use execTuples_seams as execTuples;
use execUtils_seams as execUtils;

/// The `PlanState.ExecProcNode` callback installed by `ExecInitSampleScan` (via
/// the `init_plan_state_links` seam): `castNode(SampleScanState, pstate)`, run
/// `ExecSampleScan`, and translate its `PgResult<bool>` into the
/// `ExecProcNodeMtd` `Option<SlotId>` (the produced tuple is the projection
/// result slot when projecting, else the scan tuple slot).
fn exec_sample_scan_node<'mcx>(
    pstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let node = match pstate {
        PlanStateNode::SampleScan(s) => {
            ::nodes::samplescanstate_carrier::downcast_sample_scan_state_mut::<
                SampleScanState<'mcx>,
            >(&mut **s)
            .expect("castNode(SampleScanState, pstate) failed")
        }
        other => panic!("castNode(SampleScanState, pstate) failed: {other:?}"),
    };
    let have = nodeSamplescan::ExecSampleScan(node, estate)?;
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

/// Borrow the live `EPQState` (`estate->es_epq_active`); the EPQ-recheck seams
/// are only reached inside an EvalPlanQual recheck, where it is non-NULL.
fn epq<'a, 'mcx>(estate: &'a EStateData<'mcx>) -> PgResult<&'a EPQState<'mcx>> {
    estate
        .es_epq_active
        .as_deref()
        .ok_or_else(|| PgError::error("SampleScan EPQ seam reached with es_epq_active = NULL"))
}

/// Borrow the live `EPQState` mutably.
fn epq_mut<'a, 'mcx>(estate: &'a mut EStateData<'mcx>) -> PgResult<&'a mut EPQState<'mcx>> {
    estate
        .es_epq_active
        .as_deref_mut()
        .ok_or_else(|| PgError::error("SampleScan EPQ seam reached with es_epq_active = NULL"))
}

/// `((Scan *) node->ps.plan)->scanrelid` for a `SampleScanState`.
fn sample_scanrelid<'mcx>(node: &SampleScanState<'mcx>) -> PgResult<u32> {
    let plan = node
        .ss
        .ps
        .plan
        .expect("scan_scanrelid: SampleScanState has no plan");
    Ok(plan
        .as_samplescan()
        .expect("scan_scanrelid: plan is not a SampleScan")
        .scan
        .scanrelid)
}

pub fn init_seams() {
    // --- node factory / makeNode / plan-state links ------------------------

    // samplestate->ss.ps.ExecProcNode = ExecSampleScan; (the plan back-link is
    // already set by ExecInitSampleScan in the node crate.)
    seam::init_plan_state_links::set(|samplestate, _node| {
        samplestate.ss.ps.ExecProcNode = Some(exec_sample_scan_node);
        Ok(())
    });

    // --- execUtils / execTuples init helpers -------------------------------

    // ExecAssignExprContext(estate, &samplestate->ss.ps);
    seam::exec_assign_expr_context::set(|samplestate, estate| {
        execUtils::exec_assign_expr_context::call(estate, &mut samplestate.ss.ps)
    });

    // samplestate->ss.ss_currentRelation =
    //     ExecOpenScanRelation(estate, node->scan.scanrelid, eflags);
    seam::exec_open_scan_relation::set(|samplestate, node, eflags, estate| {
        let rel = execUtils::exec_open_scan_relation::call(estate, node.scan.scanrelid, eflags)?;
        samplestate.ss_currentRelation = Some(rel);
        Ok(())
    });

    // ExecInitScanTupleSlot(estate, &samplestate->ss, RelationGetDescr(rel),
    //                       table_slot_callbacks(rel));
    seam::exec_init_scan_tuple_slot::set(|samplestate, estate| {
        let mcx = estate.es_query_cxt;
        let rel = samplestate
            .ss_currentRelation
            .as_ref()
            .expect("ExecInitSampleScan: relation not opened");
        let tts_ops = table_tableam::table_slot_callbacks(rel);
        let tupdesc = Some(mcx::alloc_in(mcx, rel.rd_att.clone_in(mcx)?)?);
        execTuples::exec_init_scan_tuple_slot::call(estate, &mut samplestate.ss, tupdesc, tts_ops)
    });

    // ExecInitResultTypeTL(&samplestate->ss.ps);
    seam::exec_init_result_type_tl::set(|samplestate, estate| {
        execTuples::exec_init_result_type_tl::call(&mut samplestate.ss.ps, estate)
    });

    // --- expression compilation (execExpr.c) -------------------------------

    // samplestate->ss.ps.qual = ExecInitQual(node->scan.plan.qual, samplestate);
    seam::exec_init_qual::set(|samplestate, node, estate| {
        let qual = node.scan.plan.qual.as_deref();
        samplestate.ss.ps.qual =
            execExpr::exec_init_qual::call(qual, &mut samplestate.ss.ps, estate)?;
        Ok(())
    });

    // samplestate->args = ExecInitExprList(tsc->args, samplestate);
    //
    // The compiled `args` field is a `PgVec<PgBox<ExprState>>`; ExecInitExprList
    // yields `PgVec<Option<ExprState>>`. Every TABLESAMPLE arg is a non-NULL
    // expression (the parser rejects NULL args), so each slot is `Some`; box and
    // collect into the node's `args` list.
    seam::exec_init_expr_list::set(|samplestate, node, estate| {
        let mcx = estate.es_query_cxt;
        let nodes: Vec<Option<&::nodes::primnodes::Expr>> = node
            .tablesample
            .as_deref()
            .and_then(|tsc| tsc.args.as_ref())
            .map(|args| args.iter().map(Some).collect())
            .unwrap_or_default();
        let states = execExpr::exec_init_expr_list::call(&nodes, &mut samplestate.ss.ps, estate)?;
        let mut out = mcx::vec_with_capacity_in(mcx, states.len())?;
        for st in states.into_iter() {
            let st = st.expect("ExecInitSampleScan: TABLESAMPLE arg compiled to a NULL ExprState");
            out.push(mcx::alloc_in(mcx, st)?);
        }
        samplestate.args = out;
        Ok(())
    });

    // samplestate->repeatable = ExecInitExpr(tsc->repeatable, samplestate);
    seam::exec_init_repeatable_expr::set(|samplestate, node, estate| {
        let repeatable = node
            .tablesample
            .as_deref()
            .and_then(|tsc| tsc.repeatable.as_deref());
        samplestate.repeatable = match repeatable {
            Some(expr) => {
                Some(execExpr::exec_init_expr::call(expr, &mut samplestate.ss.ps, estate)?)
            }
            None => None,
        };
        Ok(())
    });

    // --- per-tuple expression evaluation (tablesample_init) ----------------

    // params[i] = ExecEvalExprSwitchContext(samplestate->args[i], econtext,
    //                                       &isnull);
    seam::exec_eval_arg_in_per_tuple_context::set(|samplestate, i, is_null, estate| {
        let econtext = samplestate
            .ss
            .ps
            .ps_ExprContext
            .expect("tablesample_init: ps_ExprContext not initialized");
        let arg = samplestate
            .args
            .get_mut(i)
            .expect("tablesample_init: arg index out of range");
        let (datum, null) =
            execExpr::exec_eval_expr_switch_context::call(arg, econtext, estate)?;
        *is_null = null;
        // The TABLESAMPLE percent arg is a by-value float4; carry the raw word
        // across as the bare `datum::Datum` the sampler consumes.
        Ok(datum::datum::Datum::from_usize(datum.as_usize()))
    });

    // datum = ExecEvalExprSwitchContext(samplestate->repeatable, econtext,
    //                                   &isnull);
    seam::exec_eval_repeatable_in_per_tuple_context::set(|samplestate, is_null, estate| {
        let econtext = samplestate
            .ss
            .ps
            .ps_ExprContext
            .expect("tablesample_init: ps_ExprContext not initialized");
        let rep = samplestate
            .repeatable
            .as_mut()
            .expect("tablesample_init: repeatable evaluated but ExprState is NULL");
        let (datum, null) =
            execExpr::exec_eval_expr_switch_context::call(rep, econtext, estate)?;
        *is_null = null;
        // The REPEATABLE value is a by-value float8; carry the raw word across as
        // the bare `datum::Datum` (later fed to hashfloat8).
        Ok(datum::datum::Datum::from_usize(datum.as_usize()))
    });

    // --- PRNG / hash helpers -----------------------------------------------

    // pg_prng_uint32(&pg_global_prng_state).
    seam::pg_prng_uint32_global::set(|| Ok(prng::global_prng(|prng| prng.next_u32())));

    // DatumGetUInt32(DirectFunctionCall1(hashfloat8, datum)). The REPEATABLE
    // value is a float8 Datum (coerced by the parser); decode it to f64 and run
    // the hash (the fmgr boundary is a pure value conversion here).
    seam::hashfloat8::set(|datum| {
        let key = f64::from_bits(datum.as_u64());
        Ok(hashfunc::hashfloat8(key))
    });

    // --- table access methods (TABLESAMPLE) --------------------------------

    // samplestate->ss.ss_currentScanDesc = table_beginscan_sampling(
    //     rel, estate->es_snapshot, 0, NULL, use_bulkread, allow_sync,
    //     use_pagemode);
    seam::table_beginscan_sampling::set(|samplestate, allow_sync, estate| {
        let mcx = estate.es_query_cxt;
        let snapshot = estate.es_snapshot.clone();
        let use_bulkread = samplestate.use_bulkread;
        let use_pagemode = samplestate.use_pagemode;
        let rel = samplestate
            .ss_currentRelation
            .as_ref()
            .expect("tablesample_init: ss_currentRelation not opened");
        let scandesc = table_tableam::table_beginscan_sampling(
            mcx,
            rel,
            snapshot,
            0,
            mcx::vec_with_capacity_in(mcx, 0)?,
            use_bulkread,
            allow_sync,
            use_pagemode,
        )?;
        samplestate.ss_currentScanDesc = Some(scandesc);
        Ok(())
    });

    // table_rescan_set_params(scan, NULL, use_bulkread, allow_sync,
    //                         use_pagemode).
    seam::table_rescan_set_params::set(|samplestate, allow_sync, estate| {
        let mcx = estate.es_query_cxt;
        let use_bulkread = samplestate.use_bulkread;
        let use_pagemode = samplestate.use_pagemode;
        let scandesc = samplestate
            .ss_currentScanDesc
            .as_deref_mut()
            .expect("table_rescan_set_params: ss_currentScanDesc not set");
        table_tableam::table_rescan_set_params(
            mcx,
            scandesc,
            None,
            use_bulkread,
            allow_sync,
            use_pagemode,
        )
    });

    // table_endscan(node->ss.ss_currentScanDesc).
    seam::table_endscan::set(|samplestate| {
        if let Some(scandesc) = samplestate.ss_currentScanDesc.take() {
            table_tableam::table_endscan(scandesc)?;
        }
        Ok(())
    });

    // table_scan_sample_next_block(scan, scanstate).
    //
    // The scan descriptor lives INSIDE the node (`ss_currentScanDesc`), and the
    // node itself is the `SampleScanDriver` the AM passes through to the tsm
    // callbacks. The two borrows are disjoint (the driver methods touch only
    // `tsmroutine`/`tsm_state`, never `ss_currentScanDesc`), so move the
    // descriptor out across the call, then put it back.
    seam::table_scan_sample_next_block::set(|samplestate, estate| {
        let mcx = estate.es_query_cxt;
        let mut scandesc = samplestate
            .ss_currentScanDesc
            .take()
            .expect("table_scan_sample_next_block: ss_currentScanDesc not set");
        let res = table_tableam::table_scan_sample_next_block(
            mcx,
            &mut scandesc,
            samplestate,
        );
        samplestate.ss_currentScanDesc = Some(scandesc);
        res
    });

    // table_scan_sample_next_tuple(scan, scanstate, slot).
    seam::table_scan_sample_next_tuple::set(|samplestate, estate| {
        let mcx = estate.es_query_cxt;
        let slot_id = samplestate
            .ss
            .ss_ScanTupleSlot
            .expect("table_scan_sample_next_tuple: ss_ScanTupleSlot not initialized");
        let mut scandesc = samplestate
            .ss_currentScanDesc
            .take()
            .expect("table_scan_sample_next_tuple: ss_currentScanDesc not set");
        // The slot lives in the EState's slot table; borrow it out for the call.
        let res = {
            let slot = estate.slot_data_mut(slot_id);
            table_tableam::table_scan_sample_next_tuple(
                mcx,
                &mut scandesc,
                samplestate,
                slot,
            )
        };
        samplestate.ss_currentScanDesc = Some(scandesc);
        res
    });

    // --- per-tuple ExprContext / scan-slot plumbing (execScan.c) -----------

    // CHECK_FOR_INTERRUPTS().
    seam::check_for_interrupts::set(|| postgres_seams::check_for_interrupts::call());

    // ResetExprContext(node->ss.ps.ps_ExprContext).
    seam::reset_per_tuple_expr_context::set(|node, estate| {
        let econtext = node
            .ss
            .ps
            .ps_ExprContext
            .expect("SampleScan: ps_ExprContext not initialized");
        execUtils::reset_expr_context::call(estate, econtext)
    });

    // econtext->ecxt_scantuple = node->ss.ss_ScanTupleSlot.
    seam::set_econtext_scantuple_to_scan_slot::set(|node, estate| {
        let econtext = node
            .ss
            .ps
            .ps_ExprContext
            .expect("SampleScan: ps_ExprContext not initialized");
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
            .expect("SampleScan: ps_ExprContext not initialized");
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

    // --- EvalPlanQual machinery (execScan.c) -------------------------------

    // ((Scan *) node->ps.plan)->scanrelid.
    seam::scan_scanrelid::set(sample_scanrelid);

    // node->ps.state->es_epq_active != NULL.
    seam::es_epq_active_present::set(|_node, estate| Ok(estate.es_epq_active.is_some()));

    // bms_is_member(epqstate->epqParam, node->ps.plan->extParam).
    seam::epq_param_is_member_of_ext_param::set(|node, estate| {
        let epq_param = epq(estate)?.epqParam;
        let plan = node
            .ss
            .ps
            .plan
            .expect("epq_param_is_member_of_ext_param: SampleScanState has no plan");
        let ext_param = plan
            .as_samplescan()
            .expect("epq_param_is_member_of_ext_param: plan is not a SampleScan")
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
