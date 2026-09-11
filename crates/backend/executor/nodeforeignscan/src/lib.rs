// nodeForeignscan.c, CMD_SELECT path; FdwExecRoutine is the C FdwRoutine's
// executor half, installed per FdwKind by the provider's init_seams.
#![allow(non_snake_case)]

extern crate alloc;

use core::sync::atomic::{AtomicPtr, Ordering};

use ::execexpr::ExprState;
use ::execscan::{exec_scan, exec_scan_extended, ScanNode, ScanState};
use ::executils::{AsyncRequest, AsyncWaitCtx, EStateData, ExecSlotId};
use ::mcx::{Mcx, PgBox};
use ::types_core::{InvalidOid, Oid};
use ::types_error::PgResult;
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
    /// ExplainState bits C's hooks read (es->costs, es->verbose,
    /// es->rtable_names).
    pub explain: Option<
        for<'mcx> fn(
            &mut ForeignScanState<'mcx>,
            &mut EStateData<'mcx>,
            FdwExplainFlags<'_>,
            &mut dyn FnMut(&str, FdwExplainProp<'_>) -> PgResult<()>,
        ) -> PgResult<()>,
    >,
    /// BeginDirectModify / IterateDirectModify / EndDirectModify (fdwapi.h);
    /// None = provider has no direct modification. `iterate_direct` fills the
    /// scan slot for RETURNING rows (false = done).
    pub begin_direct: Option<
        for<'mcx> fn(&mut ForeignScanState<'mcx>, &mut EStateData<'mcx>, i32) -> PgResult<()>,
    >,
    pub iterate_direct: Option<
        for<'mcx> fn(&mut ForeignScanState<'mcx>, &mut EStateData<'mcx>) -> PgResult<bool>,
    >,
    pub end_direct: Option<
        for<'mcx> fn(&mut ForeignScanState<'mcx>, &mut EStateData<'mcx>) -> PgResult<()>,
    >,
    /// ForeignAsyncRequest / ForeignAsyncConfigureWait / ForeignAsyncNotify
    /// (fdwapi.h); None = provider is never async-capable.
    pub async_request: Option<
        for<'mcx> fn(
            &mut ForeignScanState<'mcx>,
            &mut EStateData<'mcx>,
            &mut AsyncRequest,
        ) -> PgResult<()>,
    >,
    pub async_configure_wait: Option<
        for<'mcx> fn(
            &mut ForeignScanState<'mcx>,
            &mut EStateData<'mcx>,
            &mut AsyncRequest,
            &AsyncWaitCtx,
        ) -> PgResult<()>,
    >,
    pub async_notify: Option<
        for<'mcx> fn(
            &mut ForeignScanState<'mcx>,
            &mut EStateData<'mcx>,
            &mut AsyncRequest,
        ) -> PgResult<()>,
    >,
    /// RecheckForeignScan (fdwapi.h); None = the provider relies on
    /// fdw_recheck_quals alone. The provider may store a different tuple
    /// in `slot` (a pushed-down outer join can NULL a different column set
    /// on recheck). `outer` drives the local EPQ subplan (C's
    /// outerPlanState(node)); None when the ForeignScan has no outer plan.
    pub recheck: Option<
        for<'mcx> fn(
            &mut ForeignScanState<'mcx>,
            &mut EStateData<'mcx>,
            ExecSlotId,
            Option<&mut OuterPlanDrive<'_, 'mcx>>,
        ) -> PgResult<bool>,
    >,
}

/// `ExecProcNode(outerPlanState(node))` handed to RecheckForeignScan without
/// naming the executor's PlanStateNode from this crate.
pub type OuterPlanDrive<'a, 'mcx> =
    dyn FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>> + 'a;

/// The ForeignScan's outer (EPQ alternative) subplan, owned by the executor
/// wrapper node (MaterialChild precedent).
pub trait ForeignScanOuter<'mcx> {
    fn exec_proc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>;
    /// `ExecReScan(outerPlan)`.
    fn rescan(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()>;
}

/// Drive with no outer subplan reachable (async requests: nodeAppend.c:205
/// registers async subplans only when es_epq_active == NULL, so no recheck
/// can run the EPQ subplan there).
pub enum NoOuter {}

impl<'mcx> ForeignScanOuter<'mcx> for NoOuter {
    fn exec_proc(&mut self, _estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>> {
        match *self {}
    }
    fn rescan(&mut self, _estate: &mut EStateData<'mcx>) -> PgResult<()> {
        match *self {}
    }
}

/// ForeignScanState plus its optional outer subplan for one ExecScan drive.
pub struct ForeignScanDrive<'a, 'mcx, C: ForeignScanOuter<'mcx>> {
    pub fs: &'a mut ForeignScanState<'mcx>,
    pub outer: Option<&'a mut C>,
}

/// `ExecAsyncForeignScanRequest` (nodeForeignscan.c).
pub fn exec_async_foreign_scan_request<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    areq: &mut AsyncRequest,
) -> PgResult<()> {
    let f = fdw_exec_routine(node.fdwroutine)
        .async_request
        .expect("async-capable FDW provides ForeignAsyncRequest");
    f(node, estate, areq)
}

/// `ExecAsyncForeignScanConfigureWait` (nodeForeignscan.c).
pub fn exec_async_foreign_scan_configure_wait<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    areq: &mut AsyncRequest,
    wait: &AsyncWaitCtx,
) -> PgResult<()> {
    let f = fdw_exec_routine(node.fdwroutine)
        .async_configure_wait
        .expect("async-capable FDW provides ForeignAsyncConfigureWait");
    f(node, estate, areq, wait)
}

/// `ExecAsyncForeignScanNotify` (nodeForeignscan.c).
pub fn exec_async_foreign_scan_notify<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    areq: &mut AsyncRequest,
) -> PgResult<()> {
    let f = fdw_exec_routine(node.fdwroutine)
        .async_notify
        .expect("async-capable FDW provides ForeignAsyncNotify");
    f(node, estate, areq)
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

impl<'a, 'mcx, C: ForeignScanOuter<'mcx>> ScanNode<'mcx> for ForeignScanDrive<'a, 'mcx, C> {
    #[inline(always)]
    fn ss_mut(&mut self) -> &mut ScanState<'mcx> {
        &mut self.fs.ss
    }

    /// `ForeignRecheck` (nodeForeignscan.c).
    fn epq_recheck(
        &mut self,
        estate: &mut EStateData<'mcx>,
        slot: ExecSlotId,
    ) -> PgResult<bool> {
        let ForeignScanDrive { fs, outer } = self;
        let ecxt = fs.ss.ps_ExprContext;
        // Does the tuple meet the remote qual condition?
        let e = estate.ecxt_mut(ecxt);
        e.ecxt_scantuple = Some(slot);
        e.reset();
        // If an outer join is pushed down, RecheckForeignScan may need to
        // store a different tuple in the slot, because a different set of
        // columns may go to NULL upon recheck. Otherwise, it shouldn't need
        // to change the slot contents, just return true or false to indicate
        // whether the quals still pass.
        if let Some(recheck) = fdw_exec_routine(fs.fdwroutine).recheck {
            let ok = match outer.as_deref_mut() {
                Some(o) => {
                    let mut drive = |estate: &mut EStateData<'mcx>| o.exec_proc(estate);
                    recheck(fs, estate, slot, Some(&mut drive))?
                }
                None => recheck(fs, estate, slot, None)?,
            };
            if !ok {
                return Ok(false);
            }
        }
        ::executils::exec_qual_with_subplans(fs.fdw_recheck_quals.as_deref_mut(), estate, ecxt)
    }

    fn plan_ext_param(&self) -> Option<&::types_nodes::bitmapset::Bitmapset<'mcx>> {
        Some(&self.fs.plan.scan.plan.extParam)
    }

    fn scan_next(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        let fs = &mut *self.fs;
        let routine = fdw_exec_routine(fs.fdwroutine);
        let found = if fs.plan.operation != CmdType::CMD_SELECT {
            (routine.iterate_direct.expect("direct-modify provider"))(fs, estate)?
        } else {
            (routine.iterate)(fs, estate)?
        };
        if found && fs.table_oid != InvalidOid {
            estate.slot_mut(fs.ss.ss_ScanTupleSlot).base_mut().tts_tableOid = fs.table_oid;
        }
        Ok(found)
    }
}

pub fn exec_foreign_scan<'mcx, C: ForeignScanOuter<'mcx>>(
    node: &mut ForeignScanState<'mcx>,
    outer: Option<&mut C>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    // Direct modifications cannot be re-evaluated by EvalPlanQual.
    if node.plan.operation != CmdType::CMD_SELECT && estate.es_epq_active {
        return Ok(None);
    }
    let (has_qual, has_proj) = (node.ss.qual.is_some(), node.ss.ps_ProjInfo.is_some());
    let mut drive = ForeignScanDrive { fs: node, outer };
    if estate.es_epq_active {
        return exec_scan(&mut drive, estate);
    }
    match (has_qual, has_proj) {
        (false, false) => exec_scan_extended::<_, false, false>(&mut drive, estate),
        (true, false) => exec_scan_extended::<_, true, false>(&mut drive, estate),
        (false, true) => exec_scan_extended::<_, false, true>(&mut drive, estate),
        (true, true) => exec_scan_extended::<_, true, true>(&mut drive, estate),
    }
}

pub fn exec_init_foreign_scan<'mcx>(
    mcx: Mcx<'mcx>,
    node: &'mcx ForeignScan<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<ForeignScanState<'mcx>> {
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);
    // The outer (EPQ) subplan, if any, is initialized by the executor
    // wrapper (procnode ForeignScanNode) before this runs.
    let direct = node.operation != CmdType::CMD_SELECT;
    debug_assert_eq!(direct, node.resultRelation != 0);

    let ps_ExprContext = estate.exec_assign_expr_context();
    let (rel, fdwroutine, scan_tupdesc, table_oid);
    if node.scan.scanrelid > 0 {
        let r = estate.exec_open_scan_relation(node.scan.scanrelid, eflags)?;
        fdwroutine = foreigncmds_seams::get_fdw_routine_by_rel_id::call(mcx, r.rd_id)?;
        // C copies the descriptor: FDW rows need not satisfy NOT NULL.
        scan_tupdesc = alloc::rc::Rc::new(tupdesc::CreateTupleDescCopy(mcx, &r.rd_att)?);
        table_oid = if node.fsSystemCol { r.rd_id } else { InvalidOid };
        rel = Some(r);
    } else {
        // Foreign join/upper: tuple shape comes from fdw_scan_tlist; no base
        // relation to open. (Whole-row RECORD fixup — C's
        // get_tupdesc_for_join_scan_tuples — is the provider's concern.)
        rel = None;
        fdwroutine = foreigncmds_seams::get_fdw_routine_by_server_id::call(mcx, node.fs_server)?;
        scan_tupdesc = execscan::exec_type_from_tl(mcx, &node.fdw_scan_tlist)?;
        table_oid = InvalidOid;
    }
    let ss_ScanTupleSlot =
        estate.exec_init_extra_tuple_slot(Some(scan_tupdesc), TupleSlotKind::Virtual);

    let mut ss = ScanState {
        qual: None,
        ps_ProjInfo: None,
        ps_ExprContext,
        scanrelid: node.scan.scanrelid,
        ss_currentRelation: rel,
        ss_currentScanDesc: None,
        ss_ScanTupleSlot,
        instr_idx: None,
    };
    if node.scan.scanrelid > 0 {
        execscan::exec_assign_scan_projection_info(
            mcx, estate, &mut ss, &node.scan.plan.targetlist,
        )?;
    } else {
        // ExecAssignScanProjectionInfoWithVarno(..., INDEX_VAR): the plan
        // tlist references fdw_scan_tlist positions.
        let tupdesc = estate
            .slot(ss.ss_ScanTupleSlot)
            .base()
            .tts_tupleDescriptor
            .clone()
            .expect("scan slot descriptor set above");
        ss.ps_ProjInfo = execscan::exec_conditional_assign_projection_info(
            mcx,
            estate,
            &node.scan.plan.targetlist,
            types_nodes::primnodes::INDEX_VAR as u32,
            &tupdesc,
        )?;
    }
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
    if direct {
        (fdw_exec_routine(fdwroutine).begin_direct.expect("direct-modify provider"))(
            &mut state, estate, eflags,
        )?;
    } else {
        (fdw_exec_routine(fdwroutine).begin)(&mut state, estate, eflags)?;
    }
    Ok(state)
}

pub fn exec_end_foreign_scan<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let routine = fdw_exec_routine(node.fdwroutine);
    if node.plan.operation != CmdType::CMD_SELECT {
        (routine.end_direct.expect("direct-modify provider"))(node, estate)?;
    } else {
        (routine.end)(node, estate)?;
    }
    node.fdw_state = None;
    Ok(())
}

/// `ExecReScanForeignScan`. A pushed-down join (scanrelid == 0) resets the
/// EPQ state of every base rti in fs_base_relids (ExecScanReScan
/// execScan.c:127-151). `outer` is the EPQ subplan; the caller (execami)
/// already folded any chgParam of the child into its rescan.
pub fn exec_rescan_foreign_scan<'mcx, C: ForeignScanOuter<'mcx>>(
    node: &mut ForeignScanState<'mcx>,
    outer: Option<&mut C>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Ignore direct modifications when EvalPlanQual is active --- they are
    // irrelevant for EvalPlanQual rechecking.
    if estate.es_epq_active && node.plan.operation != CmdType::CMD_SELECT {
        return Ok(());
    }
    (fdw_exec_routine(node.fdwroutine).rescan)(node, estate)?;
    // If chgParam of subnode is not null then plan will be re-scanned by
    // first ExecProcNode. outerPlan may also be NULL, in which case there is
    // nothing to rescan at all.
    if let Some(o) = outer {
        o.rescan(estate)?;
    }
    if node.plan.scan.scanrelid > 0 {
        execscan::exec_scan_rescan(&mut node.ss, estate);
    } else {
        execscan::exec_scan_rescan_relids(&mut node.ss, estate, &node.plan.fs_base_relids);
    }
    Ok(())
}

/// `show_foreignscan_info` target: the provider's ExplainForeignScan, if any.
pub fn explain_foreign_scan<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    flags: FdwExplainFlags<'_>,
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
