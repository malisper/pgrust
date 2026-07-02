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
use ::mcx::{Mcx, PgBox, PgVec};
use ::tableam::TableScanDesc;
use ::types_core::{Index, RECORDOID};
use ::types_error::PgResult;
use ::types_nodes::list::NodeList;
use ::types_nodes::node_tree::Node;
use ::types_rel::Relation;
use ::types_slot::{SlotData, TupleSlotKind};
use ::types_tuple::{CompactAttribute, FormData_pg_attribute, TupleDescData};

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
    pub ss_currentRelation: Relation<'mcx>,
    pub ss_currentScanDesc: Option<TableScanDesc<'mcx>>,
    pub ss_ScanTupleSlot: ExecSlotId,
}

/// C's `ExecScanAccessMtd` cast: the concrete node supplies the fetch; the
/// driver reaches the shared head through `ss`/`ss_mut`.
pub trait ScanNode<'mcx> {
    fn ss_mut(&mut self) -> &mut ScanState<'mcx>;
    /// Access method; stores into `ss_ScanTupleSlot`, false = end of scan.
    fn scan_next(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<bool>;
}

#[cold]
#[inline(never)]
fn interrupt_unported() -> ! {
    panic!("execscan: ProcessInterrupts (tcop/postgres.c) unported")
}

#[inline(always)]
fn check_for_interrupts() {
    if init_small::globals::InterruptPending() {
        interrupt_unported();
    }
}

#[inline(always)]
fn exec_scan_fetch<'mcx, N: ScanNode<'mcx>>(
    node: &mut N,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    check_for_interrupts();
    node.scan_next(estate)
}

/// `ExecScanExtended`: QUAL/PROJ mirror C's const-NULL argument elimination;
/// callers pass the combination resolved once at init (nodeSeqscan variants).
#[inline(always)]
pub fn exec_scan_extended<'mcx, N: ScanNode<'mcx>, const QUAL: bool, const PROJ: bool>(
    node: &mut N,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    debug_assert_eq!(QUAL, node.ss_mut().qual.is_some());
    debug_assert_eq!(PROJ, node.ss_mut().ps_ProjInfo.is_some());

    estate.ecxt_mut(node.ss_mut().ps_ExprContext).reset();
    if !QUAL && !PROJ {
        if exec_scan_fetch(node, estate)? {
            return Ok(Some(node.ss_mut().ss_ScanTupleSlot));
        }
        return Ok(None);
    }

    loop {
        if !exec_scan_fetch(node, estate)? {
            let mcx = estate.es_query_cxt;
            let ss = node.ss_mut();
            if PROJ {
                let proj = ss.ps_ProjInfo.as_ref().unwrap();
                exectuples::exec_clear_tuple(estate.slot_mut(proj.pi_result_slot), mcx);
            }
            return Ok(None);
        }

        let ss = node.ss_mut();
        let scan_id = ss.ss_ScanTupleSlot;
        estate.ecxt_mut(ss.ps_ExprContext).ecxt_scantuple = Some(scan_id);

        let passes = if QUAL {
            let mut slots = EvalSlots {
                scan: Some(estate.slot_mut(scan_id)),
                inner: None,
                outer: None,
            };
            exec_qual(ss.qual.as_deref_mut(), &mut slots)?
        } else {
            true
        };

        if passes {
            if PROJ {
                let mcx = estate.es_query_cxt;
                let proj = ss.ps_ProjInfo.as_mut().unwrap();
                let result_id = proj.pi_result_slot;
                let (scan_slot, result_slot) = slot_pair(estate, scan_id, result_id);
                let mut slots = EvalSlots { scan: Some(scan_slot), inner: None, outer: None };
                exec_project(&mut proj.pi_state, &mut slots, result_slot, mcx)?;
                return Ok(Some(result_id));
            }
            return Ok(Some(scan_id));
        }
        estate.ecxt_mut(ss.ps_ExprContext).reset();
    }
}

/// `ExecScan`: the EPQ-tolerant entry. es_epq_active lands with execMain;
/// until then this is the runtime-dispatched qual/proj combination.
pub fn exec_scan<'mcx, N: ScanNode<'mcx>>(
    node: &mut N,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    let ss = node.ss_mut();
    match (ss.qual.is_some(), ss.ps_ProjInfo.is_some()) {
        (false, false) => exec_scan_extended::<_, false, false>(node, estate),
        (true, false) => exec_scan_extended::<_, true, false>(node, estate),
        (false, true) => exec_scan_extended::<_, false, true>(node, estate),
        (true, true) => exec_scan_extended::<_, true, true>(node, estate),
    }
}

fn slot_pair<'a, 'mcx>(
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

/// `ExecScanReScan`; the es_epq_active relsubs reset arm lands with EPQState.
pub fn exec_scan_rescan<'mcx>(ss: &mut ScanState<'mcx>, estate: &mut EStateData<'mcx>) {
    let mcx = estate.es_query_cxt;
    exectuples::exec_clear_tuple(estate.slot_mut(ss.ss_ScanTupleSlot), mcx);
}

/// `ExecAssignScanProjectionInfo`: `ExecConditionalAssignProjectionInfo` over
/// the scan slot's descriptor and the Scan node's scanrelid.
pub fn exec_assign_scan_projection_info<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    ss: &mut ScanState<'mcx>,
    tlist: &NodeList<'mcx>,
) -> PgResult<()> {
    let tupdesc = estate
        .slot(ss.ss_ScanTupleSlot)
        .base()
        .tts_tupleDescriptor
        .clone()
        .expect("scan slot descriptor must be set before projection assignment");
    ss.ps_ProjInfo =
        exec_conditional_assign_projection_info(mcx, estate, tlist, ss.scanrelid, &tupdesc)?;
    Ok(())
}

pub fn exec_conditional_assign_projection_info<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    tlist: &NodeList<'mcx>,
    varno: Index,
    input_desc: &Rc<TupleDescData<'mcx>>,
) -> PgResult<Option<ProjectionInfo<'mcx>>> {
    if tlist_matches_tupdesc(tlist, varno, input_desc) {
        return Ok(None);
    }
    let result_desc = exec_type_from_tl(mcx, tlist)?;
    let result_slot = estate.exec_init_extra_tuple_slot(Some(result_desc), TupleSlotKind::Virtual);
    let pi_state = exec_build_projection_info(mcx, tlist, Some(input_desc))?;
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
    tlist: &NodeList<'mcx>,
) -> PgResult<Rc<TupleDescData<'mcx>>> {
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    for (i, item) in tlist.iter().enumerate() {
        let tle = item.as_target_entry().expect("targetlist member must be a TargetEntry");
        let typid = execexpr::expr_type(tle.expr);
        let shape = syscache_seams::lookup_pg_type_shape::call(typid)?
            .unwrap_or_else(|| panic!("cache lookup failed for type {typid}"));
        let mut att = FormData_pg_attribute {
            attnum: (i + 1) as i16,
            atttypid: typid,
            atttypmod: expr_typmod(tle.expr),
            attlen: shape.typlen,
            attbyval: shape.typbyval,
            attalign: shape.typalign,
            attstorage: shape.typstorage,
            attcollation: shape.typcollation,
            ..Default::default()
        };
        if let Some(name) = tle.resname {
            att.attname.namestrcpy(name);
        }
        compact.push(CompactAttribute::populate_from(&att));
        attrs.push(att);
    }
    Ok(Rc::new(TupleDescData {
        natts: attrs.len() as i32,
        tdtypeid: RECORDOID,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    }))
}

// exprTypmod's default arm is -1; the coercion/length arms land with their
// node families (none of which execexpr can compile yet).
fn expr_typmod(node: Node<'_>) -> i32 {
    if let Some(var) = node.as_var() {
        return var.vartypmod;
    }
    if let Some(c) = node.as_const() {
        return c.consttypmod;
    }
    -1
}
