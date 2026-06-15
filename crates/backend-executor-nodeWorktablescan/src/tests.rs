//! Tests for the `nodeWorktablescan` node logic.
//!
//! The genuinely-external operations are reached through this unit's seam
//! crate; the seam slots are process-global, so the fixtures install them once
//! and route per-test outcomes through thread-locals guarded by a serial lock.

use super::*;

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};

use mcx::MemoryContext;
use types_nodes::execnodes::ScanStateData;
use types_nodes::executor::TupleTableSlot;
use types_nodes::nodeworktablescan::RecursiveUnionStateData;

thread_local! {
    static LOG: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) };
    /// Verdict `tuplestore_gettupleslot` returns.
    static GETTUPLE: RefCell<bool> = const { RefCell::new(false) };
}

static TEST_LOCK: Mutex<()> = Mutex::new(());
static INSTALLED: AtomicBool = AtomicBool::new(false);

fn log(s: &'static str) {
    LOG.with(|l| l.borrow_mut().push(s));
}

fn log_snapshot() -> Vec<&'static str> {
    LOG.with(|l| l.borrow().clone())
}

fn install() {
    if INSTALLED.swap(true, Ordering::SeqCst) {
        return;
    }
    use seam::*;

    init_plan_state_links::set(|node, _plan, _estate| {
        log("init_plan_state_links");
        node.rustate = None;
        Ok(())
    });
    exec_assign_expr_context::set(|_, _| {
        log("exec_assign_expr_context");
        Ok(())
    });
    exec_init_result_type_tl::set(|_, _| {
        log("exec_init_result_type_tl");
        Ok(())
    });
    exec_init_scan_tuple_slot::set(|_, _| {
        log("exec_init_scan_tuple_slot");
        Ok(())
    });
    exec_init_qual::set(|_, _, _| {
        log("exec_init_qual");
        Ok(())
    });
    resolve_rustate::set(|node, estate| {
        log("resolve_rustate");
        node.rustate = Some(Box::new(RecursiveUnionStateData::new_in(estate.es_query_cxt)));
        Ok(())
    });
    exec_assign_scan_type_from_rustate::set(|_, _| {
        log("exec_assign_scan_type_from_rustate");
        Ok(())
    });
    exec_assign_scan_projection_info::set(|_, _| {
        log("exec_assign_scan_projection_info");
        Ok(())
    });
    tuplestore_gettupleslot::set(|node, estate| {
        log("tuplestore_gettupleslot");
        let loaded = GETTUPLE.with(|g| *g.borrow());
        if let Some(id) = node.ss.ss_ScanTupleSlot {
            let slot = estate.slot_mut(id);
            if loaded {
                slot.tts_flags &= !TTS_FLAG_EMPTY;
            } else {
                slot.tts_flags |= TTS_FLAG_EMPTY;
            }
        }
        Ok(loaded)
    });
    tuplestore_rescan::set(|_, _| {
        log("tuplestore_rescan");
        Ok(())
    });
    exec_clear_result_tuple_slot::set(|_, _| {
        log("exec_clear_result_tuple_slot");
        Ok(())
    });
    exec_scan_rescan::set(|_, _| {
        log("exec_scan_rescan");
        Ok(())
    });
    check_for_interrupts::set(|| Ok(()));
    es_epq_active_present::set(|_| Ok(false));
    reset_per_tuple_expr_context::set(|_, _| Ok(()));
    set_econtext_scantuple_to_scan_slot::set(|_, _| Ok(()));
    exec_clear_scan_tuple::set(|node, estate| {
        if let Some(id) = node.ss.ss_ScanTupleSlot {
            estate.slot_mut(id).tts_flags |= TTS_FLAG_EMPTY;
        }
        Ok(())
    });
    exec_clear_proj_result_slot::set(|_, _| Ok(()));
    exec_qual::set(|_, _| Ok(true));
    exec_project::set(|_, _| Ok(true));
    scan_scanrelid::set(|_| Ok(1));
    epq_param_is_member_of_ext_param::set(|_| Ok(false));
    epq_relsubs_done::set(|_, _| Ok(false));
    epq_set_relsubs_done::set(|_, _, _| Ok(()));
    epq_relsubs_slot_present::set(|_, _| Ok(false));
    epq_load_relsubs_slot::set(|_, _| Ok(()));
    epq_relsubs_rowmark_present::set(|_, _| Ok(false));
    eval_plan_qual_fetch_row_mark::set(|_, _| Ok(false));
}

fn setup() -> MutexGuard<'static, ()> {
    let g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    install();
    LOG.with(|l| l.borrow_mut().clear());
    GETTUPLE.with(|x| *x.borrow_mut() = false);
    g
}

fn empty_state<'mcx>() -> WorkTableScanStateData<'mcx> {
    WorkTableScanStateData {
        ss: ScanStateData::default(),
        rustate: None,
    }
}

// --- WorkTableScanRecheck ---

#[test]
fn recheck_always_true() {
    let _g = setup();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut st = empty_state();
    assert!(WorkTableScanRecheck(&mut st, &mut estate).unwrap());
}

// --- WorkTableScanNext ---

#[test]
fn next_loads_tuple_from_working_table() {
    let _g = setup();
    GETTUPLE.with(|x| *x.borrow_mut() = true);
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut st = empty_state();
    let qcxt = estate.es_query_cxt;
    st.ss.ss_ScanTupleSlot = Some(estate.make_slot(TupleTableSlot::new_in(qcxt)).unwrap());
    st.rustate = Some(Box::new(RecursiveUnionStateData::new_in(ctx.mcx())));

    let have = WorkTableScanNext(&mut st, &mut estate).unwrap();
    assert!(have);
    assert_eq!(
        estate.slot(st.ss.ss_ScanTupleSlot.unwrap()).tts_flags & TTS_FLAG_EMPTY,
        0
    );
}

#[test]
fn next_returns_false_when_exhausted() {
    let _g = setup();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut st = empty_state();
    let qcxt = estate.es_query_cxt;
    st.ss.ss_ScanTupleSlot = Some(estate.make_slot(TupleTableSlot::new_in(qcxt)).unwrap());
    st.rustate = Some(Box::new(RecursiveUnionStateData::new_in(ctx.mcx())));

    let have = WorkTableScanNext(&mut st, &mut estate).unwrap();
    assert!(!have);
    assert_ne!(
        estate.slot(st.ss.ss_ScanTupleSlot.unwrap()).tts_flags & TTS_FLAG_EMPTY,
        0
    );
}

// --- ExecWorkTableScan ---

#[test]
fn exec_resolves_rustate_on_first_call() {
    let _g = setup();
    GETTUPLE.with(|x| *x.borrow_mut() = true);
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut st = empty_state();
    let qcxt = estate.es_query_cxt;
    st.ss.ss_ScanTupleSlot = Some(estate.make_slot(TupleTableSlot::new_in(qcxt)).unwrap());
    st.rustate = None;

    let out = ExecWorkTableScan(&mut st, &mut estate).unwrap();
    assert!(out);
    assert!(st.rustate.is_some());
    assert_eq!(
        log_snapshot(),
        vec![
            "resolve_rustate",
            "exec_assign_scan_type_from_rustate",
            "exec_assign_scan_projection_info",
            "tuplestore_gettupleslot",
        ]
    );
}

#[test]
fn exec_skips_resolution_when_rustate_set() {
    let _g = setup();
    GETTUPLE.with(|x| *x.borrow_mut() = true);
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut st = empty_state();
    let qcxt = estate.es_query_cxt;
    st.ss.ss_ScanTupleSlot = Some(estate.make_slot(TupleTableSlot::new_in(qcxt)).unwrap());
    st.rustate = Some(Box::new(RecursiveUnionStateData::new_in(ctx.mcx())));

    let out = ExecWorkTableScan(&mut st, &mut estate).unwrap();
    assert!(out);
    assert_eq!(log_snapshot(), vec!["tuplestore_gettupleslot"]);
}

// --- ExecInitWorkTableScan ---

#[test]
fn init_builds_state_and_defers_projection() {
    let _g = setup();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    let plan = WorkTableScan::default();

    let out = ExecInitWorkTableScan(&plan, &mut estate, 0).unwrap();
    assert!(out.rustate.is_none());
    assert!(out.ss.ps.resultopsset);
    assert!(!out.ss.ps.resultopsfixed);
    assert_eq!(
        log_snapshot(),
        vec![
            "init_plan_state_links",
            "exec_assign_expr_context",
            "exec_init_result_type_tl",
            "exec_init_scan_tuple_slot",
            "exec_init_qual",
        ]
    );
}

// --- ExecReScanWorkTableScan ---

#[test]
fn rescan_rescans_tuplestore_when_rustate_set() {
    let _g = setup();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut st = empty_state();
    let qcxt = estate.es_query_cxt;
    st.ss.ps.ps_ResultTupleSlot = Some(estate.make_slot(TupleTableSlot::new_in(qcxt)).unwrap());
    st.rustate = Some(Box::new(RecursiveUnionStateData::new_in(ctx.mcx())));

    ExecReScanWorkTableScan(&mut st, &mut estate).unwrap();
    assert_eq!(
        log_snapshot(),
        vec![
            "exec_clear_result_tuple_slot",
            "exec_scan_rescan",
            "tuplestore_rescan",
        ]
    );
}

#[test]
fn rescan_skips_tuplestore_when_rustate_none() {
    let _g = setup();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut st = empty_state();
    st.ss.ps.ps_ResultTupleSlot = None;
    st.rustate = None;

    ExecReScanWorkTableScan(&mut st, &mut estate).unwrap();
    assert_eq!(log_snapshot(), vec!["exec_scan_rescan"]);
}
