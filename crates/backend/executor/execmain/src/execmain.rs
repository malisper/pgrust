use std::rc::Rc;

use ::executils::EStateData;
use ::mcx::{McxOwned, MemoryContext};
use ::tcop_dest::DestReceiver;
use ::types_core::CommandId;
use ::types_core::catalog::RELATION_RELATION_ID;
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
    querydesc::with_qd(h, standard_executor_finish)
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

// ExecCheckXactReadOnly: SELECT-only permission targets pass vacuously;
// anything that could write panics until PreventCommandIfReadOnly lands.
fn exec_check_xact_read_only(pstmt: &PlannedStmt<'_>) {
    for pi_node in pstmt.permInfos.iter() {
        let pi = pi_node.as_rte_permission_info().expect("permInfos cell");
        if pi.requiredPerms & !ACL_SELECT != 0 {
            panic!(
                "ExecCheckXactReadOnly (execMain.c): PreventCommandIfReadOnly not ported"
            );
        }
    }
    if pstmt.commandType != CmdType::CMD_SELECT || pstmt.hasModifyingCTE {
        panic!("ExecCheckXactReadOnly (execMain.c): PreventCommandIfParallelMode not ported");
    }
}

/// `ExecCheckPermissions` (ereport_on_violation arm only; no hook).
fn exec_check_permissions(pstmt: &PlannedStmt<'_>) -> PgResult<()> {
    for pi_node in pstmt.permInfos.iter() {
        let pi = pi_node.as_rte_permission_info().expect("permInfos cell");
        debug_assert!(pi.relid != 0);
        exec_check_one_rel_perms(pi)?;
    }
    Ok(())
}

/// `ExecCheckOneRelPerms`: the RTE_RELATION SELECT/INSERT/UPDATE/DELETE arms;
/// other write privileges and the column-level fallback are loud.
fn exec_check_one_rel_perms(pi: &RTEPermissionInfo<'_>) -> PgResult<()> {
    use types_nodes::parsenodes::{ACL_DELETE, ACL_UPDATE};
    let required = pi.requiredPerms;
    debug_assert!(required != 0);
    if required & !(ACL_SELECT | ACL_INSERT | ACL_UPDATE | ACL_DELETE) != 0 {
        panic!(
            "ExecCheckOneRelPerms (execMain.c): requiredPerms 0x{required:x} lane not ported"
        );
    }
    let userid = if pi.checkAsUser != 0 {
        pi.checkAsUser
    } else {
        miscinit_seams::get_user_id::call()
    };
    let r = aclchk_seams::object_aclcheck::call(RELATION_RELATION_ID, pi.relid, userid, required)?;
    if r != ACLCHECK_OK {
        panic!(
            "ExecCheckOneRelPerms (execMain.c): relation-level access denied for relation {} — \
             column-level fallback (pg_attribute_aclcheck) and aclcheck_error not ported",
            pi.relid
        );
    }
    Ok(())
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
        exec_check_xact_read_only(pstmt);
    }

    if !qd.params.is_null() || !pstmt.paramExecTypes.is_nil() {
        panic!("standard_ExecutorStart (execMain.c): ParamListInfo/ParamExecData lane not ported");
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

    // AfterTriggerBeginQuery (trigger.c): a bare query-depth bump. No CREATE
    // TRIGGER path exists, so the after-trigger queue is provably empty and
    // the begin/end pair is a no-op; revisit when trigger.c lands.
    let _after_trigger_begin_query = eflags & (EXEC_FLAG_SKIP_TRIGGERS | EXEC_FLAG_EXPLAIN_ONLY);

    let source_text = qd.source_text();
    let instrument = qd.instrument_options;
    let operation = qd.operation;

    let mut exec = McxOwned::<ExecTy>::try_new(
        MemoryContext::new_bump("ExecutorState"),
        |mcx| {
            Ok(ExecData {
                estate: EStateData::new_in(mcx),
                planstate: None,
            })
        },
    )?;
    let tup_desc = exec.with_mut_mcx(|_mcx, data| {
        // SAFETY: lifetime shortening of the read-only plan tree (PlannedStmt
        // is invariant only through its lists' GAT pointers); the retention
        // contract keeps it alive past this bundle (pquery::stmt_list shape).
        let pstmt = unsafe { querydesc::shorten_pstmt(pstmt) };
        let es = &mut data.estate;
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
    qd.exec = Some(exec);
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
        panic!("InitPlan (execMain.c): ExecRowMark lane not ported");
    }
    if !pstmt.subplans.is_nil() {
        panic!("InitPlan (execMain.c): SubPlan lane not ported (nodeSubplan.c)");
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
        exec_shutdown_node(planstate);
    }
    Ok(())
}

/// `standard_ExecutorFinish` (execMain.c).
pub fn standard_executor_finish(qd: &mut QueryDescData) -> PgResult<()> {
    let exec = qd.exec.as_mut().expect("ExecutorFinish before ExecutorStart");
    exec.with_mut(|data| {
        let es = &mut data.estate;
        debug_assert!(es.es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY == 0);
        assert!(!es.es_finished, "ExecutorFinish called twice");
        // ExecPostprocessPlan: only ModifyTable registers aux nodes, unported.
        debug_assert!(es.es_auxmodifytables.is_empty());
        // AfterTriggerEndQuery: no-op while the after-trigger queue is
        // provably empty (see AfterTriggerBeginQuery in standard_executor_start).
        es.es_finished = true;
    });
    Ok(())
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
        estate.exec_reset_tuple_table(false);
        estate.exec_close_result_relations();
        estate.exec_close_range_table_relations()?;
        snapmgr::UnregisterSnapshot(estate.es_snapshot.take().as_ref());
        snapmgr::UnregisterSnapshot(estate.es_crosscheck_snapshot.take().as_ref());
        estate.teardown();
        Ok(())
    })?;
    drop(exec);
    qd.tup_desc = None;
    Ok(())
}

// Compile-time check that the seam impls match the declared signatures.
const _: () = {
    let _: execmain_seams::executor_run::Signature = executor_run_seam;
    let _: execmain_seams::executor_start::Signature = executor_start_seam;
};
