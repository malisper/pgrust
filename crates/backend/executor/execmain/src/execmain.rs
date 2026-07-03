use std::rc::Rc;

use ::executils::EStateData;
use ::mcx::{McxOwned, MemoryContext};
use ::tcop_dest::DestReceiver;
use ::types_core::CommandId;
use ::types_error::{PgError, PgResult};
use ::types_nodes::nodes_enums::CmdType;
use ::types_nodes::parsenodes::RTEPermissionInfo;
use ::types_nodes::plannodes::PlannedStmt;
use ::types_portal::QueryDescHandle;
use ::types_scan::sdir::{ScanDirection, ScanDirectionIsNoMovement};
use ::types_slot::{
    SlotData, EXEC_FLAG_BACKWARD, EXEC_FLAG_EXPLAIN_ONLY, EXEC_FLAG_SKIP_TRIGGERS,
};
use ::types_tuple::TupleDescData;

use crate::procnode::{exec_end_node, exec_init_node, exec_proc_node, exec_shutdown_node};
use crate::querydesc::{self, ExecData, ExecTy, QueryDescData};

// One parked ExecutorState context (C's context_freelists): raw pointer keeps
// the TLS payload !needs_drop; nested executors overflow to a plain delete.
mod exec_ctx_pool {
    use ::mcx::MemoryContext;

    thread_local! {
        static SLOT: core::cell::Cell<*mut MemoryContext> =
            const { core::cell::Cell::new(core::ptr::null_mut()) };
    }

    pub(crate) fn take() -> Option<Box<MemoryContext>> {
        let p = SLOT.with(|s| s.replace(core::ptr::null_mut()));
        // SAFETY: parked via Box::into_raw below; slot nulled above (sole owner).
        (!p.is_null()).then(|| unsafe { Box::from_raw(p) })
    }

    pub(crate) fn park(ctx: Box<MemoryContext>) {
        let old = SLOT.with(|s| s.replace(Box::into_raw(ctx)));
        if !old.is_null() {
            // SAFETY: parked via Box::into_raw; displaced (nested executor) — delete.
            drop(unsafe { Box::from_raw(old) });
        }
    }
}

pub(crate) fn executor_start_seam(h: QueryDescHandle, eflags: i32) -> PgResult<()> {
    querydesc::with_qd(h, |qd| {
        backend_status_seams::pgstat_report_query_id::call(qd.plannedstmt().queryId, false);
        standard_executor_start(qd, eflags)
    })
}

pub(crate) fn executor_run_seam(
    h: QueryDescHandle,
    direction: ScanDirection,
    count: u64,
    dest: &mut DestReceiver<'_>,
) -> PgResult<()> {
    querydesc::with_qd(h, |qd| standard_executor_run(qd, direction, count, dest))
}

pub(crate) fn executor_finish_seam(h: QueryDescHandle) -> PgResult<()> {
    // The registry borrow must drop before the after-trigger firing loop:
    // RI checks re-enter the executor through SPI (fresh QueryDesc entries).
    let fire_triggers = querydesc::with_qd(h, standard_executor_finish)?;
    if fire_triggers {
        ::trigger::AfterTriggerEndQuery()?;
    }
    Ok(())
}

/// `ExecutorRewind` (execMain.c).
pub(crate) fn executor_rewind_seam(h: QueryDescHandle) -> PgResult<()> {
    querydesc::with_qd(h, |qd| {
        debug_assert_eq!(qd.operation, CmdType::CMD_SELECT);
        let exec = qd.exec.as_mut().expect("ExecutorRewind before ExecutorStart");
        exec.with_mut(|data| {
            let ExecData { estate, planstate } = data;
            let ps = planstate.as_mut().expect("ExecutorRewind without a plan state");
            crate::execami::exec_re_scan(ps, estate)
        })
    })
}

pub(crate) fn executor_end_seam(h: QueryDescHandle) -> PgResult<()> {
    querydesc::with_qd(h, standard_executor_end)
}

#[cold]
#[inline(never)]
fn unrecognized_operation(operation: CmdType) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "unrecognized operation code: {}",
        operation as i32
    )))
}

// acl.h values (transcribed like execexpr's ACL_EXECUTE).
const ACL_INSERT: u64 = 1 << 0;
const ACL_SELECT: u64 = 1 << 1;
const ACLCHECK_OK: i32 = 0;

// ExecCheckXactReadOnly (execMain.c); temp-table writes pass (session-local).
fn exec_check_xact_read_only(pstmt: &PlannedStmt<'_>) -> PgResult<()> {
    for pi_node in pstmt.permInfos.iter() {
        let pi = pi_node.as_rte_permission_info().expect("permInfos cell");
        if pi.requiredPerms & !ACL_SELECT == 0 {
            continue;
        }
        let namespace_id = syscache_seams::lookup_pg_class_ls_shape::call(pi.relid)?
            .map(|s| s.relnamespace)
            .unwrap_or(::types_core::InvalidOid);
        if namespace_seams::is_temp_namespace::call(namespace_id) {
            continue;
        }
        xact::PreventCommandIfReadOnly(create_command_name(pstmt))?;
    }
    if pstmt.commandType != CmdType::CMD_SELECT || pstmt.hasModifyingCTE {
        xact::PreventCommandIfParallelMode(create_command_name(pstmt))?;
    }
    Ok(())
}

// CreateCommandName over a PlannedStmt: the CreateCommandTag commandType arm.
fn create_command_name(pstmt: &PlannedStmt<'_>) -> &'static str {
    match pstmt.commandType {
        CmdType::CMD_SELECT => "SELECT",
        CmdType::CMD_INSERT => "INSERT",
        CmdType::CMD_UPDATE => "UPDATE",
        CmdType::CMD_DELETE => "DELETE",
        CmdType::CMD_MERGE => "MERGE",
        _ => "???",
    }
}

/// `ExecCheckPermissions` (ereport_on_violation arm only; no hook).
fn exec_check_permissions(pstmt: &PlannedStmt<'_>) -> PgResult<()> {
    for pi_node in pstmt.permInfos.iter() {
        let pi = pi_node.as_rte_permission_info().expect("permInfos cell");
        debug_assert!(pi.relid != 0);
        if !exec_check_one_rel_perms(pi)? {
            permission_denied(pi.relid)?;
        }
    }
    Ok(())
}

#[cold]
#[inline(never)]
fn permission_denied(relid: ::types_core::Oid) -> PgResult<()> {
    use types_nodes::parsenodes::ObjectType;
    // aclcheck_error(ACLCHECK_NO_PRIV, get_relkind_objtype(get_rel_relkind()),
    // get_rel_name()).
    const RELKIND_SEQUENCE: i8 = b'S' as i8;
    const RELKIND_VIEW: i8 = b'v' as i8;
    const RELKIND_MATVIEW: i8 = b'm' as i8;
    const RELKIND_FOREIGN_TABLE: i8 = b'f' as i8;
    let shape = syscache_seams::lookup_pg_class_ls_shape::call(relid)?;
    let objtype = match shape.map(|s| s.relkind) {
        Some(RELKIND_SEQUENCE) => ObjectType::OBJECT_SEQUENCE,
        Some(RELKIND_VIEW) => ObjectType::OBJECT_VIEW,
        Some(RELKIND_MATVIEW) => ObjectType::OBJECT_MATVIEW,
        Some(RELKIND_FOREIGN_TABLE) => ObjectType::OBJECT_FOREIGN_TABLE,
        _ => ObjectType::OBJECT_TABLE,
    };
    let name = syscache_seams::pg_class_relname::call(relid)?;
    let name = name
        .as_ref()
        .map(|n| core::str::from_utf8(n.name_str()).unwrap_or(""))
        .unwrap_or("");
    aclchk_seams::aclcheck_error::call(1, objtype as i32, name)
}

/// `ExecCheckOneRelPerms` (execMain.c).
fn exec_check_one_rel_perms(pi: &RTEPermissionInfo<'_>) -> PgResult<bool> {
    use types_nodes::parsenodes::ACL_UPDATE;
    const FIRST_LOW_INVALID_HEAP_ATTNUM: i32 = -7;

    let required = pi.requiredPerms;
    debug_assert!(required != 0);
    let userid = if pi.checkAsUser != 0 {
        pi.checkAsUser
    } else {
        miscinit_seams::get_user_id::call()
    };

    let rel_perms = aclchk_seams::pg_class_aclmask::call(pi.relid, userid, required, true)?;
    let remaining = required & !rel_perms;
    if remaining == 0 {
        return Ok(true);
    }

    // Only SELECT/INSERT/UPDATE can be satisfied at column level.
    if remaining & !(ACL_SELECT | ACL_INSERT | ACL_UPDATE) != 0 {
        return Ok(false);
    }

    if remaining & ACL_SELECT != 0 {
        // No column referenced (e.g. count(*)): SELECT on any column will do.
        if pi.selectedCols.is_empty()
            && aclchk_seams::pg_attribute_aclcheck_all::call(pi.relid, userid, ACL_SELECT, false)?
                != ACLCHECK_OK
        {
            return Ok(false);
        }
        let mut col = -1i32;
        loop {
            col = pi.selectedCols.next_member(col);
            if col < 0 {
                break;
            }
            let attno = col + FIRST_LOW_INVALID_HEAP_ATTNUM;
            if attno == 0 {
                // Whole-row reference: need SELECT on all columns.
                if aclchk_seams::pg_attribute_aclcheck_all::call(
                    pi.relid, userid, ACL_SELECT, true,
                )? != ACLCHECK_OK
                {
                    return Ok(false);
                }
            } else if aclchk_seams::pg_attribute_aclcheck::call(
                pi.relid,
                attno as i16,
                userid,
                ACL_SELECT,
            )? != ACLCHECK_OK
            {
                return Ok(false);
            }
        }
    }

    if remaining & ACL_INSERT != 0
        && !exec_check_permissions_modified(pi.relid, userid, &pi.insertedCols, ACL_INSERT)?
    {
        return Ok(false);
    }
    if remaining & ACL_UPDATE != 0
        && !exec_check_permissions_modified(pi.relid, userid, &pi.updatedCols, ACL_UPDATE)?
    {
        return Ok(false);
    }
    Ok(true)
}

/// `ExecCheckPermissionsModified` (execMain.c).
fn exec_check_permissions_modified(
    relid: ::types_core::Oid,
    userid: ::types_core::Oid,
    modified_cols: &::types_nodes::Bitmapset<'_>,
    required_perms: u64,
) -> PgResult<bool> {
    const FIRST_LOW_INVALID_HEAP_ATTNUM: i32 = -7;
    // No explicit column list (SELECT FOR UPDATE, corner-case UPDATEs):
    // permission on any column suffices.
    if modified_cols.is_empty() {
        return Ok(aclchk_seams::pg_attribute_aclcheck_all::call(
            relid,
            userid,
            required_perms,
            false,
        )? == ACLCHECK_OK);
    }
    let mut col = -1i32;
    loop {
        col = modified_cols.next_member(col);
        if col < 0 {
            break;
        }
        let attno = col + FIRST_LOW_INVALID_HEAP_ATTNUM;
        if attno == 0 {
            return Err(Box::new(PgError::error(
                "whole-row update is not implemented".to_string(),
            )));
        }
        if aclchk_seams::pg_attribute_aclcheck::call(relid, attno as i16, userid, required_perms)?
            != ACLCHECK_OK
        {
            return Ok(false);
        }
    }
    Ok(true)
}

/// `standard_ExecutorStart` (execMain.c).
pub fn standard_executor_start(qd: &mut QueryDescData, mut eflags: i32) -> PgResult<()> {
    assert!(qd.exec.is_none(), "ExecutorStart: query already started");
    #[cfg(debug_assertions)]
    if let Some(s) = &qd.snapshot {
        if snapmgr::ActiveSnapshotSet() {
            debug_assert!(Rc::ptr_eq(s, &snapmgr::GetActiveSnapshot()));
        }
    }
    let pstmt = qd.plannedstmt();

    if (guc_tables::vars::XactReadOnly.read() || xact::IsInParallelMode())
        && eflags & EXEC_FLAG_EXPLAIN_ONLY == 0
    {
        exec_check_xact_read_only(pstmt)?;
    }

    if !qd.query_env.is_null() {
        panic!("standard_ExecutorStart (execMain.c): QueryEnvironment wiring not ported");
    }

    let mut output_cid: CommandId = 0;
    match qd.operation {
        CmdType::CMD_SELECT => {
            if !pstmt.rowMarks.is_nil() || pstmt.hasModifyingCTE {
                output_cid = xact::GetCurrentCommandId(true)?;
            }
            if !pstmt.hasModifyingCTE {
                eflags |= EXEC_FLAG_SKIP_TRIGGERS;
            }
        }
        CmdType::CMD_INSERT | CmdType::CMD_DELETE | CmdType::CMD_UPDATE | CmdType::CMD_MERGE => {
            output_cid = xact::GetCurrentCommandId(true)?;
        }
        other => return Err(unrecognized_operation(other)),
    }

    let es_snapshot = snapmgr::RegisterSnapshot(qd.snapshot.as_ref())?;
    let es_crosscheck = snapmgr::RegisterSnapshot(qd.crosscheck_snapshot.as_ref())?;

    if eflags & (EXEC_FLAG_SKIP_TRIGGERS | EXEC_FLAG_EXPLAIN_ONLY) == 0 {
        ::trigger::AfterTriggerBeginQuery();
    }

    let source_text = qd.source_text();
    let instrument = qd.instrument_options;
    let operation = qd.operation;
    let params = qd.params;

    let ctx = exec_ctx_pool::take()
        .unwrap_or_else(|| Box::new(MemoryContext::new_bump("ExecutorState")));
    let mut exec = McxOwned::<ExecTy>::try_new_in_place_boxed(
        ctx,
        |mcx, slot| {
            let d = slot.as_mut_ptr();
            // SAFETY: field-wise init of the whole uninit slot; sret lands
            // EStateData directly in the arena (no ~1.2KB stack round trip).
            unsafe {
                (&raw mut (*d).estate).write(EStateData::new_in(mcx));
                (&raw mut (*d).planstate).write(None);
            }
            Ok(())
        },
    )?;
    let tup_desc = exec.with_mut_mcx(|_mcx, data| {
        // SAFETY: lifetime shortening of the read-only plan tree (PlannedStmt
        // is invariant only through its lists' GAT pointers); the retention
        // contract keeps it alive past this bundle (pquery::stmt_list shape).
        let pstmt = unsafe { querydesc::shorten_pstmt(pstmt) };
        let es = &mut data.estate;
        // SAFETY: the registered params live in the portal context, which
        // outlives this executor state (PortalDrop frees the handle after
        // PortalCleanup's ExecutorEnd).
        es.es_param_list_info =
            (!params.is_null()).then(|| unsafe { types_portal::params::resolve(params) });
        let n_exec = pstmt.paramExecTypes.len();
        if n_exec > 0 {
            es.es_param_exec_vals
                .try_reserve_exact(n_exec)
                .map_err(|_| _mcx.oom(n_exec))?;
            es.es_param_exec_vals
                .extend(core::iter::repeat_n(types_portal::params::ParamExecData::EMPTY, n_exec));
            es.es_param_subplans
                .try_reserve_exact(n_exec)
                .map_err(|_| _mcx.oom(n_exec))?;
            es.es_param_subplans.extend(core::iter::repeat_n(None, n_exec));
        }
        es.es_sourceText = Some(source_text);
        es.es_output_cid = output_cid;
        es.es_snapshot = es_snapshot;
        es.es_crosscheck_snapshot = es_crosscheck;
        es.es_top_eflags = eflags;
        es.es_instrument = instrument;
        es.es_jit_flags = pstmt.jitFlags;
        init_plan(data, pstmt, operation, eflags)
    })?;
    qd.tup_desc = Some(tup_desc);
    qd.exec = Some(Box::new(exec));
    Ok(())
}

/// `InitPlan` (execMain.c).
pub(crate) fn init_plan<'mcx>(
    data: &mut ExecData<'mcx>,
    pstmt: &'mcx PlannedStmt<'mcx>,
    operation: CmdType,
    eflags: i32,
) -> PgResult<Rc<TupleDescData<'static>>> {
    exec_check_permissions(pstmt)?;
    // C's bms_copy: the estate owns its pruning set (extended by ExecDoInitialPruning).
    let unpruned = pstmt
        .unprunableRelids
        .clone_in(data.estate.es_query_cxt)?;
    data.estate
        .exec_init_range_table(&pstmt.rtable, &pstmt.permInfos, unpruned)?;
    data.estate.es_plannedstmt = Some(pstmt);
    if !pstmt.partPruneInfos.is_nil() {
        panic!("ExecDoInitialPruning (execPartition.c) not ported");
    }
    if !pstmt.rowMarks.is_nil() {
        let estate = &mut data.estate;
        let n = estate.es_range_table_size as usize;
        estate.es_rowmarks.reserve(n);
        estate.es_rowmarks.extend(core::iter::repeat_n(None, n));
        for rc_node in &pstmt.rowMarks {
            let rc = rc_node.as_plan_row_mark().expect("rowMarks cell is a PlanRowMark");
            if rc.isParent {
                continue;
            }
            let rte = estate.exec_rt_fetch(rc.rti);
            if rte.rtekind == types_nodes::parsenodes::RTEKind::RTE_RELATION
                && !estate.es_unpruned_relids.is_member(rc.rti as i32)
            {
                continue;
            }
            use types_nodes::plannodes::RowMarkType::*;
            match rc.markType {
                ROW_MARK_EXCLUSIVE | ROW_MARK_NOKEYEXCLUSIVE | ROW_MARK_SHARE
                | ROW_MARK_KEYSHARE | ROW_MARK_REFERENCE => {
                    let rel = estate.exec_get_range_table_relation(rc.rti, false)?;
                    check_valid_row_mark_rel(rel, rc.markType)?;
                }
                ROW_MARK_COPY => panic!(
                    "InitPlan (execMain.c): ROW_MARK_COPY rowmark (non-relation RTE) \
                     lane not ported"
                ),
            }
            let erm = ::executils::ExecRowMark {
                relid: rte.relid,
                rti: rc.rti,
                prti: rc.prti,
                rowmarkId: rc.rowmarkId,
                markType: rc.markType,
                strength: rc.strength,
                waitPolicy: rc.waitPolicy,
                ermActive: false,
                curCtid: ::types_tuple::ItemPointerData::default(),
            };
            let cell = &mut estate.es_rowmarks[(rc.rti - 1) as usize];
            debug_assert!(cell.is_none());
            *cell = Some(erm);
        }
    }
    if !pstmt.subplans.is_nil() {
        for (i, subplan) in pstmt.subplans.iter().enumerate() {
            let mut sp_eflags = eflags
                & !(types_slot::EXEC_FLAG_REWIND
                    | EXEC_FLAG_BACKWARD
                    | types_slot::EXEC_FLAG_MARK);
            if pstmt.rewindPlanIDs.is_member((i + 1) as i32) {
                sp_eflags |= types_slot::EXEC_FLAG_REWIND;
            }
            let ps = exec_init_node(Some(subplan), &mut data.estate, sp_eflags)?
                .expect("subplans cells are plan trees");
            // Arena-cell ownership (not a struct field) so the type-erased
            // pointer never aliases a live &mut ExecData; the PlanState's Rc
            // releases run in standard_executor_end's explicit take+drop
            // (abort-path leak is the registry hazard class; see CATALOG).
            let mut cell = ::mcx::alloc_in(data.estate.es_query_cxt, Some(ps))?;
            let raw: *mut Option<crate::PlanStateNode<'_>> = &mut *cell;
            core::mem::forget(cell);
            data.estate.es_subplanstates.push(::executils::SubplanStateCell(
                // SAFETY: raw comes from a live arena allocation.
                unsafe { core::ptr::NonNull::new_unchecked(raw) }.cast(),
            ));
        }
        data.estate.es_subplan_hook = Some(crate::nodesubplan::subplan_hook);
        data.estate.es_cte_proc_hook = Some(crate::nodesubplan::cte_proc_hook);
        data.estate.es_subplan_init_hook = Some(crate::nodesubplan::subplan_expr_init_hook);
        data.estate.es_subplan_eval_hook = Some(crate::nodesubplan::subplan_expr_eval_hook);
    }

    let plan_node = pstmt.planTree.expect("PlannedStmt without planTree");
    let planstate = exec_init_node(Some(plan_node), &mut data.estate, eflags)?
        .expect("ExecInitNode of a non-NULL planTree");

    let plan = plan_node.as_plan().expect("planTree is a Plan node");
    let mut tup_type = planstate.exec_get_result_type(plan)?;

    if operation == CmdType::CMD_SELECT {
        let junk_filter_needed = plan.targetlist.iter().any(|tle_node| {
            tle_node
                .as_target_entry()
                .expect("targetlist entry is a TargetEntry")
                .resjunk
        });
        if junk_filter_needed {
            let slot = data
                .estate
                .exec_init_extra_tuple_slot(None, types_slot::TupleSlotKind::Virtual);
            let clean = crate::exec_clean_type_from_tl(&plan.targetlist)?;
            tup_type = clean.clone();
            let j = execjunk::exec_init_junk_filter(
                &mut data.estate,
                &plan.targetlist,
                clean,
                slot,
            )?;
            data.estate.es_junkFilter = Some(j);
        }
    }

    data.planstate = Some(planstate);
    Ok(tup_type)
}

/// `standard_ExecutorRun` (execMain.c).
pub fn standard_executor_run<'m>(
    qd: &mut QueryDescData,
    direction: ScanDirection,
    count: u64,
    dest: &mut DestReceiver<'m>,
) -> PgResult<()> {
    let operation = qd.operation;
    let pstmt = qd.plannedstmt();
    let send_tuples = operation == CmdType::CMD_SELECT || pstmt.hasReturning;
    // C decides parallel mode and sets already_executed inside ExecutePlan
    // (execMain.c), so a NoMovement run does neither; hoisted here only
    // because `exec` borrows qd through the closure.
    let no_movement = ScanDirectionIsNoMovement(direction);
    let use_parallel_mode = if no_movement {
        false
    } else {
        let upm =
            if qd.already_executed || count != 0 { false } else { pstmt.parallelModeNeeded };
        qd.already_executed = true;
        upm
    };
    let tup_desc = qd.tup_desc.clone();
    let exec = qd.exec.as_mut().expect("ExecutorRun before ExecutorStart");
    exec.with_mut_mcx(|_mcx, data| {
        debug_assert!(data.estate.es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY == 0);
        data.estate.es_processed = 0;
        if send_tuples {
            let desc = tup_desc.as_deref().expect("sendTuples without a result tupdesc");
            dest.startup(operation as i32, desc)?;
        }
        if !no_movement {
            execute_plan(data, operation, send_tuples, count, direction, use_parallel_mode, dest)?;
        }
        data.estate.es_total_processed += data.estate.es_processed;
        if send_tuples {
            dest.shutdown()?;
        }
        Ok(())
    })
}

/// `ExecutePlan` (execMain.c): THE per-tuple loop.
pub(crate) fn execute_plan<'m, 'mcx>(
    data: &mut ExecData<'mcx>,
    operation: CmdType,
    send_tuples: bool,
    number_tuples: u64,
    direction: ScanDirection,
    use_parallel_mode: bool,
    dest: &mut DestReceiver<'m>,
) -> PgResult<()> {
    let ExecData { estate, planstate } = data;
    let planstate = planstate.as_mut().expect("ExecutorRun without a plan state");
    estate.es_direction = direction;
    estate.es_use_parallel_mode = use_parallel_mode;
    if use_parallel_mode {
        panic!("ExecutePlan (execMain.c): EnterParallelMode lane not ported (execParallel.c)");
    }

    let mut current_tuple_count: u64 = 0;
    loop {
        estate.reset_per_tuple_expr_context();

        let Some(mut slot_id) = exec_proc_node(planstate, estate)? else {
            break;
        };

        if estate.es_junkFilter.is_some() {
            slot_id = execjunk::exec_filter_junk(estate, slot_id);
        }

        if send_tuples {
            let slot = estate.slot_mut(slot_id);
            // SAFETY: lifetime bridge at the seam boundary (C passes a raw
            // TupleTableSlot*). The receiver only copies datums out during
            // the call and retains no borrow of the slot (printtup keeps an
            // address token + its own wire buffer).
            let slot: &mut SlotData<'m> =
                unsafe { &mut *(slot as *mut SlotData<'mcx>).cast::<SlotData<'m>>() };
            if !dest.receive_slot(slot)? {
                break;
            }
        }

        if operation == CmdType::CMD_SELECT {
            estate.es_processed += 1;
        }

        current_tuple_count += 1;
        if number_tuples != 0 && number_tuples == current_tuple_count {
            break;
        }
    }

    if estate.es_top_eflags & EXEC_FLAG_BACKWARD == 0 {
        exec_shutdown_node(planstate, estate);
    }
    Ok(())
}

/// `standard_ExecutorFinish` (execMain.c).
// C fires AfterTriggerEndQuery before setting es_finished; the caller fires
// it after this returns (registry-borrow discipline) — es_finished has no
// reader during the firing loop.
pub fn standard_executor_finish(qd: &mut QueryDescData) -> PgResult<bool> {
    let exec = qd.exec.as_mut().expect("ExecutorFinish before ExecutorStart");
    exec.with_mut(|data| {
        let es = &mut data.estate;
        debug_assert!(es.es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY == 0);
        assert!(!es.es_finished, "ExecutorFinish called twice");
        // ExecPostprocessPlan: only ModifyTable registers aux nodes, unported.
        debug_assert!(es.es_auxmodifytables.is_empty());
        es.es_finished = true;
        Ok::<bool, Box<types_error::PgError>>(es.es_top_eflags & EXEC_FLAG_SKIP_TRIGGERS == 0)
    })
}

/// `standard_ExecutorEnd` (execMain.c); dropping the bundle is
/// `FreeExecutorState` (MemoryContextDelete of es_query_cxt).
pub fn standard_executor_end(qd: &mut QueryDescData) -> PgResult<()> {
    let mut exec = qd.exec.take().expect("ExecutorEnd before ExecutorStart");
    exec.with_mut(|data| -> PgResult<()> {
        let ExecData { estate, planstate } = data;
        debug_assert!(
            estate.es_finished || estate.es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY != 0
        );
        if let Some(ps) = planstate.as_mut() {
            exec_end_node(ps, estate)?;
        }
        for i in 0..estate.es_subplanstates.len() {
            let cell = estate.es_subplanstates[i];
            // SAFETY: init_plan created this arena cell; exclusive here (no
            // subplan can be mid-run during ExecutorEnd).
            let slot = unsafe {
                &mut *cell.0.cast::<Option<crate::PlanStateNode<'_>>>().as_ptr()
            };
            if let Some(mut ps) = slot.take() {
                exec_end_node(&mut ps, estate)?;
                // Dropping runs the Rc releases arena reset can't (no-drop rule).
            }
        }
        while let Some((p, dropper)) = estate.es_subplan_expr_states.pop() {
            // SAFETY: registered by exec_init_sub_plan_expr; dropped once here.
            unsafe { dropper(p) };
        }
        estate.exec_reset_tuple_table(false);
        estate.exec_close_result_relations();
        estate.exec_close_range_table_relations()?;
        snapmgr::UnregisterSnapshot(estate.es_snapshot.take().as_ref());
        snapmgr::UnregisterSnapshot(estate.es_crosscheck_snapshot.take().as_ref());
        estate.teardown();
        debug_assert!(estate.owners_released());
        Ok(())
    })?;
    // FreeExecutorState: one context reset, no per-object glue (the walk
    // above released every census-exempt owner; Drop stays the abort path);
    // the reset context parks for the next ExecutorStart (C context_freelists).
    exec_ctx_pool::park((*exec).free_recycle());
    qd.tup_desc = None;
    Ok(())
}

// Compile-time check that the seam impls match the declared signatures.
const _: () = {
    let _: execmain_seams::executor_run::Signature = executor_run_seam;
    let _: execmain_seams::executor_start::Signature = executor_start_seam;
};

// CheckValidRowMarkRel (execMain.c); the FDW arm is loud.
fn check_valid_row_mark_rel(
    rel: &::types_rel::Relation<'_>,
    mark_type: ::types_nodes::plannodes::RowMarkType,
) -> PgResult<()> {
    use ::types_nodes::plannodes::RowMarkType;
    use ::types_rel::{
        RELKIND_FOREIGN_TABLE, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
        RELKIND_SEQUENCE, RELKIND_TOASTVALUE, RELKIND_VIEW,
    };
    let what = match rel.rd_rel.relkind {
        RELKIND_RELATION | RELKIND_PARTITIONED_TABLE => return Ok(()),
        RELKIND_SEQUENCE => "sequence",
        RELKIND_TOASTVALUE => "TOAST relation",
        RELKIND_VIEW => "view",
        RELKIND_MATVIEW => {
            if mark_type == RowMarkType::ROW_MARK_REFERENCE {
                return Ok(());
            }
            "materialized view"
        }
        RELKIND_FOREIGN_TABLE => panic!(
            "CheckValidRowMarkRel (execMain.c): foreign-table RefetchForeignRow \
             probe; FDW lane"
        ),
        _ => "relation",
    };
    Err(cannot_lock_rows_in(what, rel))
}

#[cold]
#[inline(never)]
fn cannot_lock_rows_in(what: &str, rel: &::types_rel::Relation<'_>) -> Box<PgError> {
    use ::types_error::{ErrorLocation, ERRCODE_WRONG_OBJECT_TYPE};
    let relname = String::from_utf8_lossy(rel.rd_rel.relname.name_str()).into_owned();
    Box::new(
        PgError::error(format!("cannot lock rows in {what} \"{relname}\""))
            .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
            .with_error_location(ErrorLocation::new("execMain.c", 0, "CheckValidRowMarkRel")),
    )
}
