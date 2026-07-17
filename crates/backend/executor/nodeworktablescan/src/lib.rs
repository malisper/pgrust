// nodeWorktablescan.c; node->rustate is the estate-owned
// es_worktable_shared[wtParam] entry the RecursiveUnion published.
#![allow(non_snake_case)]

use ::execexpr::exec_init_qual;
use ::execscan::{exec_scan_epq, exec_scan_extended, ScanNode, ScanState};
use ::executils::{EStateData, ExecSlotId};
use ::mcx::Mcx;
use ::types_error::PgResult;
use ::types_nodes::plannodes::WorkTableScan;
use ::types_slot::{TupleSlotKind, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK};

pub fn init_seams() {}

pub struct WorkTableScanState<'mcx> {
    pub ss: ScanState<'mcx>,
    plan: &'mcx WorkTableScan<'mcx>,
    // C `rustate == NULL`: scan type + projection deferred to the first call.
    rustate_resolved: bool,
}

impl<'mcx> ScanNode<'mcx> for WorkTableScanState<'mcx> {
    #[inline(always)]
    fn ss_mut(&mut self) -> &mut ScanState<'mcx> {
        &mut self.ss
    }

    /// `WorkTableScanRecheck`: nothing to check.
    fn epq_recheck(
        &mut self,
        _estate: &mut EStateData<'mcx>,
        _slot: ExecSlotId,
    ) -> PgResult<bool> {
        Ok(true)
    }

    fn scan_next(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        debug_assert!(::types_scan::sdir::ScanDirectionIsForward(estate.es_direction));
        let mcx = estate.es_query_cxt;
        let param = self.plan.wtParam as usize;
        let mut shared = estate.worktable_shared_slot(param).take().unwrap_or_else(|| {
            panic!("WorkTableScanNext (nodeWorktablescan.c): es_worktable_shared[{param}] missing")
        });
        let got = shared
            .working_table
            .gettupleslot(true, false, estate.slot_mut(self.ss.ss_ScanTupleSlot), mcx);
        *estate.worktable_shared_slot(param) = Some(shared);
        got
    }
}

pub fn exec_work_table_scan<'mcx>(
    node: &mut WorkTableScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    if !node.rustate_resolved {
        resolve_rustate(node, estate)?;
    }
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

// The ancestor RecursiveUnion may not be initialized at our ExecInitNode
// time, so ExecAssignScanType + projection wait for the first fetch.
#[cold]
#[inline(never)]
fn resolve_rustate<'mcx>(
    node: &mut WorkTableScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let param = node.plan.wtParam as usize;
    let desc = estate
        .worktable_shared_slot(param)
        .as_ref()
        .unwrap_or_else(|| {
            panic!(
                "ExecWorkTableScan (nodeWorktablescan.c): es_worktable_shared[{param}] missing"
            )
        })
        .desc
        .clone();
    exectuples::exec_set_slot_descriptor(estate.slot_mut(node.ss.ss_ScanTupleSlot), mcx, desc);
    execscan::exec_assign_scan_projection_info(
        mcx,
        estate,
        &mut node.ss,
        &node.plan.scan.plan.targetlist,
    )?;
    node.rustate_resolved = true;
    Ok(())
}

pub fn exec_init_work_table_scan<'mcx>(
    mcx: Mcx<'mcx>,
    node: &'mcx WorkTableScan<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<WorkTableScanState<'mcx>> {
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);
    debug_assert!(node.scan.plan.lefttree.is_none() && node.scan.plan.righttree.is_none());

    let ps_ExprContext = estate.exec_assign_expr_context();
    let ss_ScanTupleSlot = estate.exec_init_extra_tuple_slot(None, TupleSlotKind::MinimalTuple);
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
    ss.qual = {
        let pb = estate.param_bind();
        ::executils::with_subplan_compile_env(estate, |env| {
            ::execexpr::exec_init_qual_subplans(mcx, &node.scan.plan.qual, pb, env)
        })?
    };

    Ok(WorkTableScanState { ss, plan: node, rustate_resolved: false })
}

pub fn exec_rescan_work_table_scan<'mcx>(
    node: &mut WorkTableScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) {
    execscan::exec_scan_rescan(&mut node.ss, estate);
    if node.rustate_resolved {
        let param = node.plan.wtParam as usize;
        if let Some(shared) = estate.worktable_shared_slot(param).as_mut() {
            shared.working_table.rescan();
        }
    }
}

mcx::forget_safe_struct!(
    WorkTableScanState<'_> { ss, plan, rustate_resolved },
);
