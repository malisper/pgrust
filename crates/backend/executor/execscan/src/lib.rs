// execScan.c + the always-inline execScan.h driver, plus the scan-coupled
// slices of execUtils.c (ExecConditionalAssignProjectionInfo,
// tlist_matches_tupdesc) and execTuples.c (ExecTypeFromTL) their home units
// deferred to this landing. EPQ arms (ExecScanFetch test-tuple substitution,
// relsubs rescan) land with execMain's EPQState; ScanState here is the
// PlanState-head subset the driver needs — execProcnode's PlanState embeds it.
#![allow(non_snake_case)]

extern crate alloc;

use alloc::rc::Rc;

use ::execexpr::{exec_build_projection_info, exec_project, exec_qual, EvalSlots, ExprState};
use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::mcx::{Mcx, PgBox};
use ::tableam::TableScanDesc;
use ::types_core::{Index, Oid, INT4OID};
use ::types_error::PgResult;
use ::types_nodes::list::NodeList;
use ::types_nodes::node_tree::Node;
use ::types_nodes::primnodes::CoercionForm;
use ::types_nodes::NodeTag;
use ::types_rel::Relation;
use ::types_slot::{SlotData, TupleSlotKind};
use ::types_tuple::TupleDescData;

pub fn init_seams() {}

#[cfg(test)]
mod tests;

pub struct ProjectionInfo<'mcx> {
    pub pi_state: PgBox<'mcx, ExprState<'mcx>>,
    pub pi_result_slot: ExecSlotId,
}

pub struct ScanState<'mcx> {
    pub qual: Option<PgBox<'mcx, ExprState<'mcx>>>,
    pub ps_ProjInfo: Option<ProjectionInfo<'mcx>>,
    pub ps_ExprContext: EcxtId,
    pub scanrelid: Index,
    // None for scans without an underlying relation (C's NULL: FunctionScan).
    pub ss_currentRelation: Option<Relation<'mcx>>,
    pub ss_currentScanDesc: Option<TableScanDesc<'mcx>>,
    pub ss_ScanTupleSlot: ExecSlotId,
    // es_instrumentation slot (C ps.instrument); InstrCountFiltered's target.
    pub instr_idx: Option<u32>,
}

/// C's `ExecScanAccessMtd` cast: the concrete node supplies the fetch.
pub trait ScanNode<'mcx> {
    fn ss_mut(&mut self) -> &mut ScanState<'mcx>;
    /// Access method; stores into `ss_ScanTupleSlot`, false = end of scan.
    fn scan_next(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<bool>;
    /// C `ExecScanRecheckMtd` over an EPQ test tuple already in `slot`.
    fn epq_recheck(
        &mut self,
        _estate: &mut EStateData<'mcx>,
        _slot: ExecSlotId,
    ) -> PgResult<bool> {
        panic!(
            "ExecScanFetch (execScan.h): EPQ recheck for {} not ported",
            core::any::type_name::<Self>()
        );
    }
}

#[cold]
#[inline(never)]
fn process_interrupts() -> PgResult<()> {
    postgres_seams::check_for_interrupts::call()
}

#[inline(always)]
fn check_for_interrupts() -> PgResult<()> {
    if init_small::globals::InterruptPending() {
        return process_interrupts();
    }
    Ok(())
}

enum EpqFetch {
    Tuple(ExecSlotId),
    Empty,
    FallThrough,
}

// ExecScanFetch's es_epq_active arm: test-tuple substitution.
fn epq_fetch<'mcx, N: ScanNode<'mcx>>(
    node: &mut N,
    estate: &mut EStateData<'mcx>,
) -> PgResult<EpqFetch> {
    let scanrelid = node.ss_mut().scanrelid;
    assert!(
        scanrelid > 0,
        "ExecScanFetch (execScan.h): scanrelid == 0 EPQ arm (FDW/CustomScan \
         join pushdown) not ported"
    );
    let idx = (scanrelid - 1) as usize;
    let mcx = estate.es_query_cxt;
    let subs = estate.es_epq.as_mut().expect("EPQ scan variant under an installed EPQ state");
    if subs.relsubs_done[idx] {
        let ss_slot = node.ss_mut().ss_ScanTupleSlot;
        exectuples::exec_clear_tuple(estate.slot_mut(ss_slot), mcx);
        return Ok(EpqFetch::Empty);
    }
    if let Some(test) = subs.relsubs_slot[idx] {
        subs.relsubs_done[idx] = true;
        if estate.slot(test).base().is_empty() {
            return Ok(EpqFetch::Empty);
        }
        if !node.epq_recheck(estate, test)? {
            exectuples::exec_clear_tuple(estate.slot_mut(test), mcx);
            return Ok(EpqFetch::Empty);
        }
        return Ok(EpqFetch::Tuple(test));
    }
    if let Some(rm) = subs.relsubs_rowmark[idx] {
        subs.relsubs_done[idx] = true;
        let orig = subs.origslot.expect("EPQ rowmark fetch requires EvalPlanQualSetSlot");
        return epq_fetch_row_mark(node, estate, rm, orig);
    }
    // No test tuple and no rowmark: a plain rescannable rel falls through.
    Ok(EpqFetch::FallThrough)
}

// EvalPlanQualFetchRowMark (execMain.c), non-locking marks only: re-return
// the origslot row's junk ctid (ROW_MARK_REFERENCE, refetched under
// SnapshotAny) or wholerow datum (ROW_MARK_COPY) through the scan slot.
fn epq_fetch_row_mark<'mcx, N: ScanNode<'mcx>>(
    node: &mut N,
    estate: &mut EStateData<'mcx>,
    rm: ::executils::EpqRowMarkFetch,
    orig: ExecSlotId,
) -> PgResult<EpqFetch> {
    let mcx = estate.es_query_cxt;
    let ss_slot = node.ss_mut().ss_ScanTupleSlot;
    let scanrelid = node.ss_mut().scanrelid;
    let mut isnull = false;
    match rm {
        ::executils::EpqRowMarkFetch::Reference { ctid_attno } => {
            let datum = exectuples::slot_getattr(
                estate.slot_mut(orig),
                ctid_attno as i32,
                &mut isnull,
            );
            if isnull {
                exectuples::exec_clear_tuple(estate.slot_mut(ss_slot), mcx);
                return Ok(EpqFetch::Empty);
            }
            // SAFETY: a tid datum points at an ItemPointerData inside the
            // origslot's materialized tuple, live for this row.
            let tid = unsafe { *(datum.as_usize() as *const types_tuple::ItemPointerData) };
            let snapshot_any = Some(std::rc::Rc::new(types_snapshot::SnapshotData::sentinel(
                mcx,
                types_snapshot::SNAPSHOT_ANY,
            )));
            let found = {
                let EStateData { es_relations, es_tupleTable, .. } = estate;
                let rel = es_relations[(scanrelid - 1) as usize]
                    .as_ref()
                    .expect("rowmark relation opened at InitPlan");
                tableam::table_tuple_fetch_row_version(
                    mcx,
                    rel,
                    &tid,
                    &snapshot_any,
                    &mut es_tupleTable[ss_slot.0 as usize],
                )?
            };
            if !found {
                return Err(Box::new(::types_error::PgError::error(
                    "failed to fetch tuple for EvalPlanQual recheck",
                )));
            }
        }
        ::executils::EpqRowMarkFetch::Copy { whole_attno } => {
            let datum = exectuples::slot_getattr(
                estate.slot_mut(orig),
                whole_attno as i32,
                &mut isnull,
            );
            if isnull {
                exectuples::exec_clear_tuple(estate.slot_mut(ss_slot), mcx);
                return Ok(EpqFetch::Empty);
            }
            // ExecStoreHeapTupleDatum: DatumGetHeapTupleHeader detoasts first —
            // a wholerow datum that crossed a Hash/Sort/Material tuple store is
            // repacked as a short varlena (no 4B header); the detoasted copy
            // lives in the query context like C's.
            let src = datum.as_usize() as *const u8;
            // SAFETY: a non-null wholerow junk datum is a live varlena image,
            // valid in the origslot for this row.
            let hdr = unsafe {
                if !types_tuple::varatt::varatt_is_4b_u(src) {
                    let image = core::slice::from_raw_parts(
                        src,
                        types_tuple::varatt::varsize_any(src),
                    );
                    let flat = ::detoast_seams::detoast_attr::call(mcx, image)?;
                    let p = flat.as_ptr();
                    core::mem::forget(flat);
                    p
                } else {
                    src
                }
            };
            // SAFETY: hdr is a plain 4B composite image readable for its
            // datum length.
            let t_len = unsafe {
                (*(hdr as *const types_tuple::htup::HeapTupleHeaderData)).datum_length()
            };
            let mut tid = types_tuple::ItemPointerData::default();
            types_tuple::itemptr::ItemPointerSetInvalid(&mut tid);
            // SAFETY: image bounds established above.
            let tup = unsafe {
                types_tuple::HeapTupleData::from_raw_parts(hdr, t_len, tid, types_core::InvalidOid)
            };
            exectuples::exec_force_store_heap_tuple(tup, estate.slot_mut(ss_slot), mcx)?;
        }
    }
    if !node.epq_recheck(estate, ss_slot)? {
        exectuples::exec_clear_tuple(estate.slot_mut(ss_slot), mcx);
        return Ok(EpqFetch::Empty);
    }
    Ok(EpqFetch::Tuple(ss_slot))
}

#[inline(always)]
fn exec_scan_fetch<'mcx, N: ScanNode<'mcx>, const EPQ: bool>(
    node: &mut N,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    check_for_interrupts()?;
    if EPQ {
        match epq_fetch(node, estate)? {
            EpqFetch::Tuple(id) => return Ok(Some(id)),
            EpqFetch::Empty => return Ok(None),
            EpqFetch::FallThrough => {}
        }
    }
    if node.scan_next(estate)? {
        return Ok(Some(node.ss_mut().ss_ScanTupleSlot));
    }
    Ok(None)
}

/// `ExecScanExtended`: QUAL/PROJ mirror C's const-NULL argument elimination.
#[inline(always)]
pub fn exec_scan_extended<'mcx, N: ScanNode<'mcx>, const QUAL: bool, const PROJ: bool>(
    node: &mut N,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    exec_scan_impl::<N, QUAL, PROJ, false>(node, estate)
}

/// `ExecScanExtended` with a live `epqstate` (the ExecSeqScanEPQ shape).
pub fn exec_scan_epq<'mcx, N: ScanNode<'mcx>>(
    node: &mut N,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    let ss = node.ss_mut();
    match (ss.qual.is_some(), ss.ps_ProjInfo.is_some()) {
        (false, false) => exec_scan_impl::<_, false, false, true>(node, estate),
        (true, false) => exec_scan_impl::<_, true, false, true>(node, estate),
        (false, true) => exec_scan_impl::<_, false, true, true>(node, estate),
        (true, true) => exec_scan_impl::<_, true, true, true>(node, estate),
    }
}

#[inline(always)]
fn exec_scan_impl<'mcx, N: ScanNode<'mcx>, const QUAL: bool, const PROJ: bool, const EPQ: bool>(
    node: &mut N,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    debug_assert_eq!(QUAL, node.ss_mut().qual.is_some());
    debug_assert_eq!(PROJ, node.ss_mut().ps_ProjInfo.is_some());

    estate.ecxt_mut(node.ss_mut().ps_ExprContext).reset();
    if !QUAL && !PROJ {
        return exec_scan_fetch::<_, EPQ>(node, estate);
    }

    loop {
        let Some(scan_id) = exec_scan_fetch::<_, EPQ>(node, estate)? else {
            let mcx = estate.es_query_cxt;
            let ss = node.ss_mut();
            if PROJ {
                let proj = ss.ps_ProjInfo.as_ref().unwrap();
                exectuples::exec_clear_tuple(estate.slot_mut(proj.pi_result_slot), mcx);
            }
            return Ok(None);
        };

        let ss = node.ss_mut();
        estate.ecxt_mut(ss.ps_ExprContext).ecxt_scantuple = Some(scan_id);

        // ExecEvalParamExec pending-initplan arm, hoisted out of the interpreter.
        if QUAL {
            let deps = ss.qual.as_deref().unwrap().param_exec_deps();
            if !deps.is_empty() {
                executils::exec_eval_param_exec_params(estate, deps)?;
            }
        }

        let ss = node.ss_mut();
        let passes = if QUAL {
            if ss.qual.as_deref().is_some_and(|q| q.has_subplan()) {
                let ecxt = ss.ps_ExprContext;
                executils::exec_qual_with_subplans(ss.qual.as_deref_mut(), estate, ecxt)?
            } else {
                // Per-tuple result mcx for arg-detoasting quals (C's
                // ecxt_per_tuple_memory; ExprContext reset frees it).
                let per_tuple = estate.ecxt(ss.ps_ExprContext).per_tuple_mcx();
                // SAFETY: reset-only context, outlives the plan.
                unsafe {
                    ss.qual.as_deref_mut().unwrap().arm_result_mcx_raw(per_tuple);
                }
                let mut slots = EvalSlots {
                    scan: Some(estate.slot_mut(scan_id)),
                    inner: None,
                    outer: None,
                };
                exec_qual(ss.qual.as_deref_mut(), &mut slots)?
            }
        } else {
            true
        };

        if passes {
            if PROJ {
                // C reads projection initplan params inside the projection,
                // which never runs on a qual-rejected tuple.
                {
                    let deps = ss.ps_ProjInfo.as_ref().unwrap().pi_state.param_exec_deps();
                    if !deps.is_empty() {
                        executils::exec_eval_param_exec_params(estate, deps)?;
                    }
                }
                let ss = node.ss_mut();
                let ecxt = ss.ps_ExprContext;
                let proj = ss.ps_ProjInfo.as_mut().unwrap();
                let result_id = proj.pi_result_slot;
                if proj.pi_state.has_subplan() {
                    executils::exec_project_with_subplans(
                        &mut proj.pi_state,
                        estate,
                        ecxt,
                        result_id,
                    )?;
                    return Ok(Some(result_id));
                }
                // By-ref projection results (and callee scratch — e.g.
                // regexp_replace's wchar buffer) must live in the per-tuple
                // memory reset at the next exec_scan entry (C projects into
                // ecxt_per_tuple_memory); es_query_cxt here accumulated
                // ~26GB anon over one 100M-row per-row-regexp_replace scan.
                // SAFETY: reset-only context, outlives the plan.
                unsafe {
                    let per_tuple = estate.ecxt(ecxt).per_tuple_mcx();
                    proj.pi_state.arm_result_mcx_raw(per_tuple);
                }
                let mcx = estate.es_query_cxt;
                let (scan_slot, result_slot) = slot_pair(estate, scan_id, result_id);
                let mut slots = EvalSlots { scan: Some(scan_slot), inner: None, outer: None };
                ::execexpr::exec_project_prearmed(&mut proj.pi_state, &mut slots, result_slot, mcx)?;
                return Ok(Some(result_id));
            }
            return Ok(Some(scan_id));
        }
        if let Some(idx) = ss.instr_idx {
            estate.es_instrumentation[idx as usize].nfiltered1 += 1.0;
        }
        estate.ecxt_mut(ss.ps_ExprContext).reset();
    }
}

/// `ExecScan`: reads `es_epq_active` per call (execScan.c) — under an EPQ
/// recheck the fetch substitutes the locked test tuple instead of running
/// the access method.
pub fn exec_scan<'mcx, N: ScanNode<'mcx>>(
    node: &mut N,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    if estate.es_epq_active {
        return exec_scan_epq(node, estate);
    }
    let ss = node.ss_mut();
    match (ss.qual.is_some(), ss.ps_ProjInfo.is_some()) {
        (false, false) => exec_scan_extended::<_, false, false>(node, estate),
        (true, false) => exec_scan_extended::<_, true, false>(node, estate),
        (false, true) => exec_scan_extended::<_, false, true>(node, estate),
        (true, true) => exec_scan_extended::<_, true, true>(node, estate),
    }
}

// ===========================================================================
// Lane-executor-v2 scan-driver seam: `exec_scan_impl`'s per-tuple
// qual/projection body over ONE already-produced tuple. The lane's push chain
// supplies the tuple (a SubqueryScan's subplan row arrives from an upstream
// lane pipeline instead of `scan_next`), so the fetch is elided; everything
// else mirrors the QUAL/PROJ arms of `exec_scan_impl` exactly — per-tuple
// expr-context reset at entry, `ecxt_scantuple` staging, pending-initplan
// param hoists (projection params only on qual-passing rows), the
// subplan-aware qual/projection drivers, per-tuple result-mcx arming for the
// non-subplan paths, and the nfiltered1 instrumentation tick on a rejected
// row. Kept separate from the const-generic `exec_scan_impl` so the hot
// per-tuple scan codegen is untouched; the arms are runtime-dispatched here
// (the lane is not per-instruction sensitive at this seam).
// ===========================================================================

/// One pushed tuple through the scan driver's qual → project segment.
/// `None` = qual-rejected (the caller feeds the next tuple); `Some(slot)` =
/// the projected output (or `scan_id` itself for projection-less scans).
pub fn lane_scan_accept<'mcx>(
    ss: &mut ScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    scan_id: ExecSlotId,
) -> PgResult<Option<ExecSlotId>> {
    estate.ecxt_mut(ss.ps_ExprContext).reset();
    estate.ecxt_mut(ss.ps_ExprContext).ecxt_scantuple = Some(scan_id);

    if ss.qual.is_some() {
        // ExecEvalParamExec pending-initplan arm, hoisted out of the
        // interpreter (exec_scan_impl parity).
        let deps = ss.qual.as_deref().unwrap().param_exec_deps();
        if !deps.is_empty() {
            executils::exec_eval_param_exec_params(estate, deps)?;
        }
        let passes = if ss.qual.as_deref().is_some_and(|q| q.has_subplan()) {
            let ecxt = ss.ps_ExprContext;
            executils::exec_qual_with_subplans(ss.qual.as_deref_mut(), estate, ecxt)?
        } else {
            // Per-tuple result mcx for arg-detoasting quals (C's
            // ecxt_per_tuple_memory; the entry reset frees it).
            let per_tuple = estate.ecxt(ss.ps_ExprContext).per_tuple_mcx();
            // SAFETY: reset-only context, outlives the plan.
            unsafe {
                ss.qual.as_deref_mut().unwrap().arm_result_mcx_raw(per_tuple);
            }
            let mut slots = EvalSlots {
                scan: Some(estate.slot_mut(scan_id)),
                inner: None,
                outer: None,
            };
            exec_qual(ss.qual.as_deref_mut(), &mut slots)?
        };
        if !passes {
            if let Some(idx) = ss.instr_idx {
                estate.es_instrumentation[idx as usize].nfiltered1 += 1.0;
            }
            return Ok(None);
        }
    }

    let ecxt = ss.ps_ExprContext;
    let Some(proj) = ss.ps_ProjInfo.as_mut() else {
        return Ok(Some(scan_id));
    };
    // C reads projection initplan params inside the projection, which never
    // runs on a qual-rejected tuple.
    {
        let deps = proj.pi_state.param_exec_deps();
        if !deps.is_empty() {
            executils::exec_eval_param_exec_params(estate, deps)?;
        }
    }
    let result_id = proj.pi_result_slot;
    if proj.pi_state.has_subplan() {
        executils::exec_project_with_subplans(&mut proj.pi_state, estate, ecxt, result_id)?;
        return Ok(Some(result_id));
    }
    // By-ref projection results (and callee scratch) must live in the
    // per-tuple memory reset at the next entry (exec_scan_impl parity).
    // SAFETY: reset-only context, outlives the plan.
    unsafe {
        let per_tuple = estate.ecxt(ecxt).per_tuple_mcx();
        proj.pi_state.arm_result_mcx_raw(per_tuple);
    }
    let mcx = estate.es_query_cxt;
    let (scan_slot, result_slot) = slot_pair(estate, scan_id, result_id);
    let mut slots = EvalSlots { scan: Some(scan_slot), inner: None, outer: None };
    ::execexpr::exec_project_prearmed(&mut proj.pi_state, &mut slots, result_slot, mcx)?;
    Ok(Some(result_id))
}

pub fn slot_pair<'a, 'mcx>(
    estate: &'a mut EStateData<'mcx>,
    a: ExecSlotId,
    b: ExecSlotId,
) -> (&'a mut SlotData<'mcx>, &'a mut SlotData<'mcx>) {
    let (i, j) = (a.0 as usize, b.0 as usize);
    debug_assert_ne!(i, j);
    let slots = &mut estate.es_tupleTable[..];
    if i < j {
        let (lo, hi) = slots.split_at_mut(j);
        (&mut lo[i], &mut hi[0])
    } else {
        let (lo, hi) = slots.split_at_mut(i);
        (&mut hi[0], &mut lo[j])
    }
}

/// `ExecScanReScan`.
pub fn exec_scan_rescan<'mcx>(ss: &mut ScanState<'mcx>, estate: &mut EStateData<'mcx>) {
    let mcx = estate.es_query_cxt;
    exectuples::exec_clear_tuple(estate.slot_mut(ss.ss_ScanTupleSlot), mcx);
    if estate.es_epq_active {
        assert!(ss.scanrelid > 0, "ExecScanReScan (execScan.c): scanrelid == 0 EPQ reset not ported");
        let idx = (ss.scanrelid - 1) as usize;
        let subs = estate.es_epq.as_mut().expect("EPQ rescan under an installed EPQ state");
        subs.relsubs_done[idx] = subs.relsubs_blocked[idx];
    }
}

/// `ExecAssignScanProjectionInfo`: `ExecConditionalAssignProjectionInfo` over
/// the scan slot's descriptor and the Scan node's scanrelid.
pub fn exec_assign_scan_projection_info<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    ss: &mut ScanState<'mcx>,
    tlist: &NodeList<'mcx>,
) -> PgResult<()> {
    exec_assign_scan_projection_info_parent(mcx, estate, ss, tlist, None)
}

/// [`exec_assign_scan_projection_info`] for SubqueryScan/CteScan: the subplan
/// targetlist feeds whole-row junk filtering (C ExprState.parent).
pub fn exec_assign_scan_projection_info_parent<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    ss: &mut ScanState<'mcx>,
    tlist: &NodeList<'mcx>,
    parent_subplan_tlist: Option<&NodeList<'mcx>>,
) -> PgResult<()> {
    let tupdesc = estate
        .slot(ss.ss_ScanTupleSlot)
        .base()
        .tts_tupleDescriptor
        .clone()
        .expect("scan slot descriptor must be set before projection assignment");
    ss.ps_ProjInfo = exec_conditional_assign_projection_info_parent(
        mcx,
        estate,
        tlist,
        ss.scanrelid,
        &tupdesc,
        parent_subplan_tlist,
    )?;
    Ok(())
}

pub fn exec_conditional_assign_projection_info<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    tlist: &NodeList<'mcx>,
    varno: Index,
    input_desc: &Rc<TupleDescData<'mcx>>,
) -> PgResult<Option<ProjectionInfo<'mcx>>> {
    exec_conditional_assign_projection_info_parent(mcx, estate, tlist, varno, input_desc, None)
}

pub fn exec_conditional_assign_projection_info_parent<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    tlist: &NodeList<'mcx>,
    varno: Index,
    input_desc: &Rc<TupleDescData<'mcx>>,
    parent_subplan_tlist: Option<&NodeList<'mcx>>,
) -> PgResult<Option<ProjectionInfo<'mcx>>> {
    if tlist_matches_tupdesc(tlist, varno, input_desc) {
        return Ok(None);
    }
    let result_desc = exec_type_from_tl(mcx, tlist)?;
    let result_slot = estate.exec_init_extra_tuple_slot(Some(result_desc), TupleSlotKind::Virtual);
    let params = estate.param_bind();
    let pi_state =
        executils::with_subplan_compile_env_parent(estate, parent_subplan_tlist, |env| {
            execexpr::exec_build_projection_info_subplans(mcx, tlist, Some(input_desc), params, env)
        })?;
    Ok(Some(ProjectionInfo { pi_state, pi_result_slot: result_slot }))
}

fn tlist_matches_tupdesc(tlist: &NodeList<'_>, varno: Index, tupdesc: &TupleDescData<'_>) -> bool {
    let mut items = tlist.iter();
    for attrno in 1..=tupdesc.natts {
        let Some(item) = items.next() else {
            return false;
        };
        let tle = item.as_target_entry().expect("targetlist member must be a TargetEntry");
        let Some(var) = tle.expr.as_var() else {
            return false;
        };
        debug_assert_eq!(var.varno, varno as i32);
        debug_assert_eq!(var.varlevelsup, 0);
        if var.varattno as i32 != attrno {
            return false;
        }
        let att = &tupdesc.attrs[(attrno - 1) as usize];
        if att.attisdropped || att.atthasmissing {
            return false;
        }
        if var.vartype != att.atttypid
            || (var.vartypmod != att.atttypmod && var.vartypmod != -1)
        {
            return false;
        }
    }
    items.next().is_none()
}

/// `ExecTypeFromTL` (execTuples.c), skipJunk = false.
pub fn exec_type_from_tl<'mcx>(
    mcx: Mcx<'mcx>,
    tlist: &NodeList<'_>,
) -> PgResult<Rc<TupleDescData<'mcx>>> {
    exec_type_from_tl_internal(mcx, tlist, false)
}

/// `ExecCleanTypeFromTL` (execTuples.c): resjunk columns omitted.
pub fn exec_clean_type_from_tl<'mcx>(
    mcx: Mcx<'mcx>,
    tlist: &NodeList<'_>,
) -> PgResult<Rc<TupleDescData<'mcx>>> {
    exec_type_from_tl_internal(mcx, tlist, true)
}

fn tle<'mcx>(node: Node<'mcx>) -> &'mcx types_nodes::primnodes::TargetEntry<'mcx> {
    node.as_target_entry()
        .unwrap_or_else(|| panic!("expected TargetEntry, got tag {:?}", node.node_tag()))
}

// plancache computes result descriptors over pre-planner tlists, which can
// still carry raw SubLinks; execexpr's exprType covers plan-tree families
// only, so route SubLink to the canonical nodeFuncs port.
fn tl_expr_type(e: Node<'_>) -> Oid {
    if e.node_tag() == NodeTag::T_SubLink {
        return nodes_core::expr_type(e);
    }
    execexpr::expr_type(e)
}

/// `ExecTypeFromExprList` (execTuples.c): bare exprs, unnamed columns.
pub fn exec_type_from_expr_list<'mcx>(
    mcx: Mcx<'mcx>,
    exprs: &NodeList<'_>,
) -> PgResult<Rc<TupleDescData<'mcx>>> {
    let mut desc = tupdesc::CreateTemplateTupleDesc(mcx, exprs.len() as i32)?;
    let mut cur_resno: i16 = 1;
    for e in exprs.iter() {
        tupdesc::TupleDescInitEntry(
            &mut desc,
            cur_resno,
            None,
            tl_expr_type(e),
            expr_typmod(e),
            0,
        )?;
        tupdesc::TupleDescInitEntryCollation(&mut desc, cur_resno, expr_collation(e));
        cur_resno += 1;
    }
    Ok(Rc::new(desc))
}

fn exec_type_from_tl_internal<'mcx>(
    mcx: Mcx<'mcx>,
    tlist: &NodeList<'_>,
    skipjunk: bool,
) -> PgResult<Rc<TupleDescData<'mcx>>> {
    let len = tlist.iter().filter(|&n| !(skipjunk && tle(n).resjunk)).count();
    let mut desc = tupdesc::CreateTemplateTupleDesc(mcx, len as i32)?;
    let mut cur_resno: i16 = 1;
    for node in tlist.iter() {
        let t = tle(node);
        if skipjunk && t.resjunk {
            continue;
        }
        tupdesc::TupleDescInitEntry(
            &mut desc,
            cur_resno,
            t.resname,
            tl_expr_type(t.expr),
            expr_typmod(t.expr),
            0,
        )?;
        tupdesc::TupleDescInitEntryCollation(&mut desc, cur_resno, expr_collation(t.expr));
        cur_resno += 1;
    }
    Ok(Rc::new(desc))
}

/// C `exprTypmod` over the ported primnode families.
pub fn expr_typmod(node: Node<'_>) -> i32 {
    match node.node_tag() {
        NodeTag::T_Var => node.as_var().unwrap().vartypmod,
        NodeTag::T_Const => node.as_const().unwrap().consttypmod,
        NodeTag::T_Param => node.as_param().unwrap().paramtypmod,
        NodeTag::T_FuncExpr => {
            let f = node.as_func_expr().unwrap();
            length_coercion_typmod(f).unwrap_or(-1)
        }
        NodeTag::T_OpExpr
        | NodeTag::T_Aggref
        | NodeTag::T_GroupingFunc
        | NodeTag::T_WindowFunc
        | NodeTag::T_BoolExpr
        | NodeTag::T_NullTest
        | NodeTag::T_BooleanTest
        | NodeTag::T_DistinctExpr
        | NodeTag::T_RowExpr => -1,
        NodeTag::T_SubPlan => {
            use types_nodes::primnodes::SubLinkType;
            let sp = node.as_sub_plan().unwrap();
            match sp.subLinkType {
                SubLinkType::EXPR_SUBLINK | SubLinkType::ARRAY_SUBLINK => sp.firstColTypmod,
                _ => -1,
            }
        }
        NodeTag::T_CaseTestExpr => node.as_case_test_expr().unwrap().typeMod,
        // C exprTypmod CaseExpr: typmod only when every result agrees.
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            let Some(defresult) = c.defresult else { return -1 };
            if execexpr::expr_type(defresult) != c.casetype {
                return -1;
            }
            let typmod = expr_typmod(defresult);
            if typmod < 0 {
                return -1;
            }
            for w in &c.args {
                let result =
                    w.as_case_when().expect("CaseWhen").result.expect("CaseWhen.result");
                if execexpr::expr_type(result) != c.casetype || expr_typmod(result) != typmod {
                    return -1;
                }
            }
            typmod
        }
        NodeTag::T_RelabelType => node.as_relabel_type().unwrap().resulttypmod,
        NodeTag::T_CoerceViaIO => -1,
        NodeTag::T_NextValueExpr => -1,
        NodeTag::T_CoerceToDomain => node.as_coerce_to_domain().unwrap().resulttypmod,
        NodeTag::T_CoerceToDomainValue => node.as_coerce_to_domain_value().unwrap().typeMod,
        _ => nodes_core::expr_typmod(node),
    }
}

// C exprIsLengthCoercion: cast-form call, second arg a non-null int4 Const typmod.
fn length_coercion_typmod(f: &types_nodes::primnodes::FuncExpr<'_>) -> Option<i32> {
    match f.funcformat {
        CoercionForm::COERCE_EXPLICIT_CAST | CoercionForm::COERCE_IMPLICIT_CAST => {}
        _ => return None,
    }
    if !(2..=3).contains(&f.args.len()) {
        return None;
    }
    let second = f.args.iter().nth(1)?;
    let con = second.as_const()?;
    if con.consttype != INT4OID || con.constisnull {
        return None;
    }
    Some(con.constvalue.as_i32())
}

// Exempt: droppy owners, released by execmain's exec_end_node/end_scan.
mcx::forget_safe_struct!(
    ProjectionInfo<'_> { pi_result_slot; pi_state },
    ScanState<'_> { ps_ExprContext, scanrelid, ss_ScanTupleSlot, instr_idx;
        qual, ps_ProjInfo, ss_currentRelation, ss_currentScanDesc },
);

/// C `exprCollation` over the ported primnode families.
pub fn expr_collation(node: Node<'_>) -> Oid {
    match node.node_tag() {
        NodeTag::T_Var => node.as_var().unwrap().varcollid,
        NodeTag::T_Const => node.as_const().unwrap().constcollid,
        NodeTag::T_Param => node.as_param().unwrap().paramcollid,
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().funccollid,
        NodeTag::T_OpExpr => node.as_op_expr().unwrap().opcollid,
        NodeTag::T_Aggref => node.as_aggref().unwrap().aggcollid,
        NodeTag::T_GroupingFunc => 0,
        NodeTag::T_WindowFunc => node.as_window_func().unwrap().wincollid,
        NodeTag::T_SubPlan => {
            use types_nodes::primnodes::SubLinkType;
            let sp = node.as_sub_plan().unwrap();
            match sp.subLinkType {
                SubLinkType::EXPR_SUBLINK | SubLinkType::ARRAY_SUBLINK => sp.firstColCollation,
                _ => 0,
            }
        }
        NodeTag::T_CaseExpr => node.as_case_expr().unwrap().casecollid,
        NodeTag::T_CaseTestExpr => node.as_case_test_expr().unwrap().collation,
        NodeTag::T_RelabelType => node.as_relabel_type().unwrap().resultcollid,
        NodeTag::T_CoerceViaIO => node.as_coerce_via_io().unwrap().resultcollid,
        NodeTag::T_BoolExpr
        | NodeTag::T_NullTest
        | NodeTag::T_BooleanTest
        | NodeTag::T_RowExpr
        | NodeTag::T_NextValueExpr => ::types_core::InvalidOid,
        NodeTag::T_DistinctExpr => node.as_distinct_expr().unwrap().opcollid,
        NodeTag::T_CoerceToDomain => node.as_coerce_to_domain().unwrap().resultcollid,
        NodeTag::T_CoerceToDomainValue => node.as_coerce_to_domain_value().unwrap().collation,
        _ => nodes_core::expr_collation(node),
    }
}
