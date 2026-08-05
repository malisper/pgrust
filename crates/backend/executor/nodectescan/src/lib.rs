// nodeCtescan.c; the C leader alias (cte_table/eof_cte via the cteParam
// slot) is the estate-owned es_cte_shared[cteParam] entry.
#![allow(non_snake_case)]

extern crate alloc;

use alloc::rc::Rc;

use ::execexpr::exec_init_qual;
use ::execscan::{exec_scan_epq, exec_scan_extended, ScanNode, ScanState};
use ::executils::{CteShared, EStateData, ExecSlotId};
use ::mcx::Mcx;
use ::types_error::PgResult;
use ::types_nodes::list::NodeList;
use ::types_nodes::plannodes::CteScan;
use ::types_slot::{TupleSlotKind, EXEC_FLAG_MARK, EXEC_FLAG_REWIND};
use ::types_tuple::TupleDescData;
use ::tuplestore::Tuplestore;

pub fn init_seams() {}

pub struct CteScanState<'mcx> {
    pub ss: ScanState<'mcx>,
    readptr: i32,
    cte_plan_id: i32,
    cte_param: i32,
    is_leader: bool,
}

impl<'mcx> ScanNode<'mcx> for CteScanState<'mcx> {
    #[inline(always)]
    fn ss_mut(&mut self) -> &mut ScanState<'mcx> {
        &mut self.ss
    }

    /// `CteScanRecheck`: nothing to check.
    fn epq_recheck(
        &mut self,
        _estate: &mut EStateData<'mcx>,
        _slot: ExecSlotId,
    ) -> PgResult<bool> {
        Ok(true)
    }

    // Take-out keeps the tuplestore and slot borrows disjoint.
    fn scan_next(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        let param = self.cte_param as usize;
        let mut shared = estate.cte_shared_slot(param).take().unwrap_or_else(|| {
            panic!("CteScanNext (nodeCtescan.c): es_cte_shared[{param}] missing")
        });
        let result = self.next_inner(&mut shared, estate);
        *estate.cte_shared_slot(param) = Some(shared);
        result
    }
}

impl<'mcx> CteScanState<'mcx> {
    /// Forward-only (backward-execution wave B9): C nodeCtescan.c's backward
    /// arms - the `!forward && eof_tuplestore` skip-back and the
    /// direction-aware tuplestore_gettupleslot - are deleted; the run seam
    /// refuses backward entry (deletion-prep B1). The SHARED tuplestore's
    /// own backward capability stays (rider row 12 - the store serve path;
    /// sibling CteScans re-read it via their own read pointers, forward).
    fn next_inner(
        &mut self,
        shared: &mut CteShared,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<bool> {
        debug_assert!(
            ::types_scan::sdir::ScanDirectionIsForward(estate.es_direction),
            "backward drive below the forward-only run seam (deletion-prep B1)"
        );
        let mcx = estate.es_query_cxt;
        let ts = &mut shared.tuplestore;
        ts.select_read_pointer(self.readptr)?;

        let mut eof_tuplestore = ts.ateof();

        if !eof_tuplestore {
            let slot = estate.slot_mut(self.ss.ss_ScanTupleSlot);
            if ts.gettupleslot(true, true, slot, mcx)? {
                return Ok(true);
            }
            eof_tuplestore = true;
        }

        if eof_tuplestore && !shared.eof_cte {
            let hook = estate
                .es_cte_proc_hook
                .expect("CteScanNext before execmain installed es_cte_proc_hook");
            let cell = estate.es_subplanstates[(self.cte_plan_id - 1) as usize];
            // SAFETY: cell installed by execmain's InitPlan on this estate.
            let pulled = unsafe { hook(cell, estate) }?;
            let Some(sub_slot) = pulled else {
                shared.eof_cte = true;
                exectuples::exec_clear_tuple(estate.slot_mut(self.ss.ss_ScanTupleSlot), mcx);
                return Ok(false);
            };

            let ts = &mut shared.tuplestore;
            ts.select_read_pointer(self.readptr)?;
            // Our EOF pointer is active: it advances over this append.
            ts.puttupleslot(estate.slot_mut(sub_slot), mcx)?;
            shared.fills += 1;

            // ExecCopySlot: output must survive other CteScans advancing.
            let mtup = exectuples::exec_copy_slot_minimal_tuple(
                estate.slot_mut(sub_slot),
                mcx,
                mcx,
                0,
            )?;
            let scan = estate.slot_mut(self.ss.ss_ScanTupleSlot);
            exectuples::exec_store_minimal_tuple_owned(scan, mcx, mtup);
            return Ok(true);
        }

        exectuples::exec_clear_tuple(estate.slot_mut(self.ss.ss_ScanTupleSlot), mcx);
        Ok(false)
    }
}

/// show_ctescan_info's leader->cte_table read (the shared estate slot here).
pub fn storage_stats(
    node: &CteScanState<'_>,
    estate: &mut EStateData<'_>,
) -> Option<types_core::instrument::TuplestoreInstrumentation> {
    estate
        .cte_shared_slot(node.cte_param as usize)
        .as_mut()
        .map(|shared| shared.tuplestore.get_stats())
}

pub fn exec_cte_scan<'mcx>(
    node: &mut CteScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    // C ExecScan reads es_epq_active per call (see nodefunctionscan).
    if estate.es_epq_active {
        return exec_scan_epq(node, estate);
    }
    match (node.ss.qual.is_some(), node.ss.ps_ProjInfo.is_some()) {
        (false, false) => exec_scan_extended::<_, false, false>(node, estate),
        (true, false) => exec_scan_extended::<_, true, false>(node, estate),
        (false, true) => exec_scan_extended::<_, false, true>(node, estate),
        (true, true) => exec_scan_extended::<_, true, true>(node, estate),
    }
}

/// `scan_desc` is ExecGetResultType(cteplanstate) — caller-computed.
pub fn exec_init_cte_scan<'mcx>(
    mcx: Mcx<'mcx>,
    node: &CteScan<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
    scan_desc: Rc<TupleDescData<'static>>,
    // The CTE plan's targetlist (C cteplanstate->plan->targetlist): whole-row
    // junk filtering in the projection/qual compile.
    sub_tlist: &NodeList<'mcx>,
) -> PgResult<CteScanState<'mcx>> {
    debug_assert!(eflags & EXEC_FLAG_MARK == 0);
    // C forces REWIND: any node may be asked to rescan the shared store.
    let eflags = eflags | EXEC_FLAG_REWIND;
    debug_assert!(node.scan.plan.lefttree.is_none() && node.scan.plan.righttree.is_none());

    let param = node.cteParam as usize;
    debug_assert!(!estate.es_param_exec_vals[param].exec_plan);
    let (readptr, is_leader) = match estate.cte_shared_slot(param) {
        slot @ None => {
            let mut ts = Tuplestore::begin_heap(true, false, init_small::globals::work_mem());
            ts.set_eflags(eflags);
            *slot = Some(CteShared { tuplestore: ts, eof_cte: false, fills: 0 });
            (0, true)
        }
        Some(shared) => {
            let ts = &mut shared.tuplestore;
            let p = ts.alloc_read_pointer(eflags);
            ts.select_read_pointer(p)?;
            ts.rescan()?;
            (p, false)
        }
    };

    let ps_ExprContext = estate.exec_assign_expr_context();
    let ss_ScanTupleSlot =
        estate.exec_init_extra_tuple_slot(Some(scan_desc), TupleSlotKind::MinimalTuple);
    let mut ss = ScanState {
        qual: None,
        ps_ProjInfo: None,
        ps_ExprContext,
        scanrelid: node.scan.scanrelid,
        ss_currentRelation: None,
        ss_currentScanDesc: None,
        ss_ScanTupleSlot,
        instr_idx: None,
    };
    execscan::exec_assign_scan_projection_info_parent(
        mcx,
        estate,
        &mut ss,
        &node.scan.plan.targetlist,
        Some(sub_tlist),
    )?;
    ss.qual = {
        let pb = estate.param_bind();
        ::executils::with_subplan_compile_env_parent(estate, Some(sub_tlist), |env| {
            ::execexpr::exec_init_qual_subplans(mcx, &node.scan.plan.qual, pb, env)
        })?
    };

    Ok(CteScanState {
        ss,
        readptr,
        cte_plan_id: node.ctePlanId,
        cte_param: node.cteParam,
        is_leader,
    })
}

pub fn exec_end_cte_scan<'mcx>(node: &mut CteScanState<'mcx>, estate: &mut EStateData<'mcx>) {
    if node.is_leader {
        if let Some(shared) = estate.cte_shared_slot(node.cte_param as usize).take() {
            shared.tuplestore.end();
        }
    }
}

pub fn exec_rescan_cte_scan<'mcx>(
    node: &mut CteScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    execscan::exec_scan_rescan(&mut node.ss, estate);
    let param = node.cte_param as usize;
    let shared = estate
        .cte_shared_slot(param)
        .as_mut()
        .unwrap_or_else(|| panic!("ExecReScanCteScan: es_cte_shared[{param}] missing"));
    let ts = &mut shared.tuplestore;
    ts.select_read_pointer(node.readptr)?;
    ts.rescan()?;
    Ok(())
}

/// cteParam in chg == C's leader-cteplanstate-chgParam test; redundant
/// clears across followers are fine per C.
pub fn exec_rescan_cte_scan_chg<'mcx>(
    node: &mut CteScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    chg: &::types_nodes::bitmapset::Bitmapset<'mcx>,
) -> PgResult<()> {
    if !chg.is_member(node.cte_param) {
        return exec_rescan_cte_scan(node, estate);
    }
    execscan::exec_scan_rescan(&mut node.ss, estate);
    let param = node.cte_param as usize;
    let shared = estate
        .cte_shared_slot(param)
        .as_mut()
        .unwrap_or_else(|| panic!("ExecReScanCteScan: es_cte_shared[{param}] missing"));
    shared.tuplestore.clear();
    shared.eof_cte = false;
    Ok(())
}

mcx::forget_safe_struct!(
    CteScanState<'_> { ss, readptr, cte_plan_id, cte_param, is_leader },
);
