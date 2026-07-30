// nodeForeignscan.c, CMD_SELECT path; FdwExecRoutine is the C FdwRoutine's
// executor half, installed per FdwKind by the provider's init_seams.
#![allow(non_snake_case)]

extern crate alloc;

use core::sync::atomic::{AtomicPtr, Ordering};

use ::execexpr::{exec_qual, EvalSlots, ExprState};
use ::execscan::{exec_scan, exec_scan_extended, ScanNode, ScanState};
use ::executils::{EStateData, ExecSlotId};
use ::mcx::{Mcx, PgBox};
use ::types_core::{InvalidOid, Oid};
use ::types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use ::types_nodes::plannodes::ForeignScan;
use ::types_nodes::{CmdType, FdwExplainFlags, FdwExplainProp, FdwKind, NUM_FDW_KINDS};
use ::types_slot::{TupleSlotKind, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK};

pub fn init_seams() {}

pub struct ForeignScanState<'mcx> {
    pub ss: ScanState<'mcx>,
    pub plan: &'mcx ForeignScan<'mcx>,
    pub fdw_recheck_quals: Option<PgBox<'mcx, ExprState<'mcx>>>,
    pub fdwroutine: FdwKind,
    table_oid: Oid,
    /// C fdw_state (funcapi_srf user_fctx precedent), dropped at end-scan.
    pub fdw_state: Option<Box<dyn core::any::Any>>,
}

/// C FdwRoutine's exec half; `iterate` fills the scan slot (false = EOF).
pub struct FdwExecRoutine {
    pub begin:
        for<'mcx> fn(&mut ForeignScanState<'mcx>, &mut EStateData<'mcx>, i32) -> PgResult<()>,
    pub iterate:
        for<'mcx> fn(&mut ForeignScanState<'mcx>, &mut EStateData<'mcx>) -> PgResult<bool>,
    pub rescan:
        for<'mcx> fn(&mut ForeignScanState<'mcx>, &mut EStateData<'mcx>) -> PgResult<()>,
    pub end: for<'mcx> fn(&mut ForeignScanState<'mcx>, &mut EStateData<'mcx>) -> PgResult<()>,
    /// Emits FdwExplainProp, not ExplainState; FdwExplainFlags carries the
    /// ExplainState bits C's hooks read (es->costs, es->verbose).
    pub explain: Option<
        for<'mcx> fn(
            &mut ForeignScanState<'mcx>,
            &mut EStateData<'mcx>,
            FdwExplainFlags,
            &mut dyn FnMut(&str, FdwExplainProp<'_>) -> PgResult<()>,
        ) -> PgResult<()>,
    >,
}

static FDW_EXEC_ROUTINES: [AtomicPtr<FdwExecRoutine>; NUM_FDW_KINDS] =
    [const { AtomicPtr::new(core::ptr::null_mut()) }; NUM_FDW_KINDS];

pub fn install_fdw_exec_routine(kind: FdwKind, routine: &'static FdwExecRoutine) {
    FDW_EXEC_ROUTINES[kind.index()]
        .store(routine as *const FdwExecRoutine as *mut FdwExecRoutine, Ordering::Release);
}

fn fdw_exec_routine(kind: FdwKind) -> &'static FdwExecRoutine {
    let p = FDW_EXEC_ROUTINES[kind.index()].load(Ordering::Acquire);
    assert!(
        !p.is_null(),
        "nodeforeignscan: no FdwExecRoutine installed for {kind:?} \
         (provider init_seams missing)"
    );
    // SAFETY: install stores a &'static FdwExecRoutine; never unset.
    unsafe { &*p }
}

impl<'mcx> ScanNode<'mcx> for ForeignScanState<'mcx> {
    #[inline(always)]
    fn ss_mut(&mut self) -> &mut ScanState<'mcx> {
        &mut self.ss
    }

    /// `ForeignRecheck` (RecheckForeignScan callback unmodeled: no provider).
    fn epq_recheck(
        &mut self,
        estate: &mut EStateData<'mcx>,
        slot: ExecSlotId,
    ) -> PgResult<bool> {
        let ecxt = self.ss.ps_ExprContext;
        estate.ecxt_mut(ecxt).ecxt_scantuple = Some(slot);
        let passes = {
            let per_tuple = estate.ecxt(ecxt).per_tuple_mcx();
            if let Some(q) = self.fdw_recheck_quals.as_deref_mut() {
                // SAFETY: reset-only context, outlives the plan.
                unsafe { q.arm_result_mcx_raw(per_tuple) };
            }
            let mut slots = EvalSlots {
                scan: Some(estate.slot_mut(slot)),
                inner: None,
                outer: None,
            };
            exec_qual(self.fdw_recheck_quals.as_deref_mut(), &mut slots)?
        };
        estate.ecxt_mut(ecxt).reset();
        Ok(passes)
    }

    fn scan_next(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        let routine = fdw_exec_routine(self.fdwroutine);
        let found = (routine.iterate)(self, estate)?;
        if found && self.table_oid != InvalidOid {
            estate.slot_mut(self.ss.ss_ScanTupleSlot).base_mut().tts_tableOid = self.table_oid;
        }
        Ok(found)
    }
}

pub fn exec_foreign_scan<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    if estate.es_epq_active {
        return exec_scan(node, estate);
    }
    match (node.ss.qual.is_some(), node.ss.ps_ProjInfo.is_some()) {
        (false, false) => exec_scan_extended::<_, false, false>(node, estate),
        (true, false) => exec_scan_extended::<_, true, false>(node, estate),
        (false, true) => exec_scan_extended::<_, false, true>(node, estate),
        (true, true) => exec_scan_extended::<_, true, true>(node, estate),
    }
}

#[track_caller]
#[cold]
#[inline(never)]
fn foreign_scan_unported(what: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("{what} is not yet implemented"))
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

pub fn exec_init_foreign_scan<'mcx>(
    mcx: Mcx<'mcx>,
    node: &'mcx ForeignScan<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<ForeignScanState<'mcx>> {
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);
    // unported: ExecInitForeignScan (nodeForeignscan.c) direct-modify, FDW
    // outer-plan, and fdw_scan_tlist lanes raise clean feature errors.
    if node.operation != CmdType::CMD_SELECT || node.resultRelation != 0 {
        return Err(foreign_scan_unported("direct modification of a foreign table"));
    }
    if node.scan.plan.lefttree.is_some() {
        return Err(foreign_scan_unported("a foreign scan with an outer subplan"));
    }
    if node.scan.scanrelid == 0 || !node.fdw_scan_tlist.is_nil() {
        return Err(foreign_scan_unported("a foreign scan with a custom scan tuple list"));
    }

    let ps_ExprContext = estate.exec_assign_expr_context();
    let rel = estate.exec_get_range_table_relation(node.scan.scanrelid, false)?.alias();
    let fdwroutine = foreigncmds_seams::get_fdw_routine_by_rel_id::call(mcx, rel.rd_id)?;
    // C copies the descriptor: FDW rows need not satisfy NOT NULL.
    let scan_tupdesc = tupdesc::CreateTupleDescCopy(mcx, &rel.rd_att)?;
    let ss_ScanTupleSlot = estate.exec_init_extra_tuple_slot(
        Some(alloc::rc::Rc::new(scan_tupdesc)),
        TupleSlotKind::Virtual,
    );
    let table_oid = if node.fsSystemCol { rel.rd_id } else { InvalidOid };

    let mut ss = ScanState {
        qual: None,
        ps_ProjInfo: None,
        ps_ExprContext,
        scanrelid: node.scan.scanrelid,
        ss_currentRelation: Some(rel),
        ss_currentScanDesc: None,
        ss_ScanTupleSlot,
        instr_idx: None,
    };
    execscan::exec_assign_scan_projection_info(mcx, estate, &mut ss, &node.scan.plan.targetlist)?;
    ss.qual = {
        let pb = estate.param_bind();
        ::executils::with_subplan_compile_env(estate, |env| {
            ::execexpr::exec_init_qual_subplans(mcx, &node.scan.plan.qual, pb, env)
        })?
    };
    let fdw_recheck_quals = {
        let pb = estate.param_bind();
        ::executils::with_subplan_compile_env(estate, |env| {
            ::execexpr::exec_init_qual_subplans(mcx, &node.fdw_recheck_quals, pb, env)
        })?
    };

    let mut state = ForeignScanState {
        ss,
        plan: node,
        fdw_recheck_quals,
        fdwroutine,
        table_oid,
        fdw_state: None,
    };
    (fdw_exec_routine(fdwroutine).begin)(&mut state, estate, eflags)?;
    Ok(state)
}

pub fn exec_end_foreign_scan<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    (fdw_exec_routine(node.fdwroutine).end)(node, estate)?;
    node.fdw_state = None;
    Ok(())
}

/// `ExecReScanForeignScan` (outerPlan arm dead: init refuses pushdown plans).
pub fn exec_rescan_foreign_scan<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    (fdw_exec_routine(node.fdwroutine).rescan)(node, estate)?;
    execscan::exec_scan_rescan(&mut node.ss, estate);
    Ok(())
}

/// `show_foreignscan_info` target: the provider's ExplainForeignScan, if any.
pub fn explain_foreign_scan<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    flags: FdwExplainFlags,
    emit: &mut dyn FnMut(&str, FdwExplainProp<'_>) -> PgResult<()>,
) -> PgResult<()> {
    match fdw_exec_routine(node.fdwroutine).explain {
        Some(f) => f(node, estate, flags, emit),
        None => Ok(()),
    }
}

mcx::forget_safe_struct!(
    // Exempt: droppy ExprState carrier + provider state (ScanState precedent).
    ForeignScanState<'_> { ss, plan, fdwroutine, table_oid; fdw_recheck_quals, fdw_state },
);
